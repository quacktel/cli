package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	mu    sync.Mutex
	calls []string
	// reply maps a launchctl verb to its canned output and error.
	reply map[string]fakeReply
	// seq overrides reply: replies are consumed in order and the last repeats.
	seq map[string][]fakeReply
}

const goneOut = `Could not find service "dev.quacktel.test" in domain for user gui: 501`

var errGone = errors.New("exit status 113")

type fakeReply struct {
	out string
	err error
}

func (f *fakeRunner) run(_ context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.Join(args, " "))
	r := f.reply[args[0]]
	if q := f.seq[args[0]]; len(q) > 0 {
		r = q[0]
		if len(q) > 1 {
			f.seq[args[0]] = q[1:]
		}
	}

	return []byte(r.out), r.err
}

func newManager(t *testing.T, f *fakeRunner) *Manager {
	t.Helper()

	if _, ok := f.reply["print"]; !ok && f.seq["print"] == nil {
		if f.reply == nil {
			f.reply = map[string]fakeReply{}
		}
		f.reply["print"] = fakeReply{out: goneOut, err: errGone}
	}

	return &Manager{
		Label: "dev.quacktel.test", Home: t.TempDir(), UID: 501, Run: f.run,
		pollInterval: time.Millisecond, waitTimeout: 200 * time.Millisecond, retryBackoff: time.Millisecond,
	}
}

func TestRenderEscapesAndRoundTrips(t *testing.T) {
	in := Plist{
		Label:            "dev.quacktel.test",
		ProgramArguments: []string{"/bin/q", "--x", `a&b<c>"d'`},
		LogPath:          "/tmp/launchd.log",
	}
	data, err := Render(in)
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, "a&amp;b&lt;c&gt;&#34;d&#39;")
	require.NotContains(t, out, `a&b<c`)
	require.Contains(t, out, `<plist version="1.0">`)
	require.Contains(t, out, "<key>ThrottleInterval</key>\n\t\t<integer>30</integer>")
	// launchd fails to bootstrap a plist with <true></true>, so booleans must be self-closing.
	require.Contains(t, out, "<key>RunAtLoad</key>\n\t\t<true/>")
	require.Contains(t, out, "<key>SuccessfulExit</key>\n\t\t\t<false/>")
	require.NotContains(t, out, "<true></true>")
	require.NotContains(t, out, "<false></false>")

	path := filepath.Join(t.TempDir(), "p.plist")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	got, err := ReadPlist(path)
	require.NoError(t, err)
	require.Equal(t, in, got)
}

func TestInstall(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"bootout": {out: "Boot-out failed: 3: No such process", err: errors.New("exit status 3")},
	}}
	m := newManager(t, f)
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q", "serve", "--auth"}))

	info, err := os.Stat(m.PlistPath())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	logInfo, err := os.Stat(filepath.Join(m.Home, "Library", "Logs", "quacktel"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), logInfo.Mode().Perm())
	require.Equal(t, []string{
		"bootout gui/501/dev.quacktel.test",
		"print gui/501/dev.quacktel.test",
		"bootstrap gui/501 " + m.PlistPath(),
	}, f.calls)

	// A second install replaces the plist and reloads.
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q", "serve", "--retention", "24h"}))
	p, err := ReadPlist(m.PlistPath())
	require.NoError(t, err)
	require.Equal(t, []string{"/bin/q", "serve", "--retention", "24h"}, p.ProgramArguments)
	require.Len(t, f.calls, 6)
	require.Equal(t, "bootout gui/501/dev.quacktel.test", f.calls[3])

	entries, err := os.ReadDir(filepath.Dir(m.PlistPath()))
	require.NoError(t, err)
	require.Len(t, entries, 1)
}

func TestInstallBootstrapFailure(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"bootstrap": {out: "Bootstrap failed: 5: Input/output error", err: errors.New("exit status 5")},
	}}
	m := newManager(t, f)
	err := m.Install(t.Context(), []string{"/bin/q"})
	require.ErrorContains(t, err, "Input/output error")
}

func TestInstallWaitsForUnload(t *testing.T) {
	loaded := fakeReply{out: printSample}
	f := &fakeRunner{seq: map[string][]fakeReply{
		"print": {loaded, loaded, {out: goneOut, err: errGone}},
	}}
	m := newManager(t, f)
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q"}))
	require.Equal(t, []string{
		"bootout gui/501/dev.quacktel.test",
		"print gui/501/dev.quacktel.test",
		"print gui/501/dev.quacktel.test",
		"print gui/501/dev.quacktel.test",
		"bootstrap gui/501 " + m.PlistPath(),
	}, f.calls)
}

func TestInstallRetriesBootstrapStatus5(t *testing.T) {
	f := &fakeRunner{seq: map[string][]fakeReply{
		"bootstrap": {{out: "Bootstrap failed: 5: Input/output error", err: errors.New("exit status 5")}, {}},
	}}
	m := newManager(t, f)
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q"}))
	require.Equal(t, 2, countCalls(f, "bootstrap"))
}

func TestInstallBootstrapPersistentFailureRetriesThenErrors(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"bootstrap": {out: "Bootstrap failed: 5: Input/output error", err: errors.New("exit status 5")},
	}}
	m := newManager(t, f)
	err := m.Install(t.Context(), []string{"/bin/q"})
	require.ErrorContains(t, err, "Bootstrap failed: 5")
	require.Equal(t, 5, countCalls(f, "bootstrap"))
}

func TestInstallOtherBootstrapErrorIsNotRetried(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"bootstrap": {out: "boom", err: errors.New("exit status 1")},
	}}
	m := newManager(t, f)
	require.ErrorContains(t, m.Install(t.Context(), []string{"/bin/q"}), "boom")
	require.Equal(t, 1, countCalls(f, "bootstrap"))
}

func TestInstallWaitDeadline(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{"print": {out: printSample}}}
	m := newManager(t, f)
	err := m.Install(t.Context(), []string{"/bin/q"})
	require.ErrorContains(t, err, "waiting for dev.quacktel.test to unload")
	require.Zero(t, countCalls(f, "bootstrap"))
}

func TestUninstallWaitsForUnload(t *testing.T) {
	f := &fakeRunner{seq: map[string][]fakeReply{
		"print": {{out: printSample}, {out: goneOut, err: errGone}},
	}}
	m := newManager(t, f)
	require.NoError(t, m.Uninstall(t.Context()))
	require.Equal(t, 2, countCalls(f, "print"))

	f = &fakeRunner{reply: map[string]fakeReply{"print": {out: printSample}}}
	m = newManager(t, f)
	require.ErrorContains(t, m.Uninstall(t.Context()), "to unload")
}

func countCalls(f *fakeRunner, verb string) int {
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, verb+" ") {
			n++
		}
	}

	return n
}

func TestUninstall(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"bootout": {out: "Could not find service", err: errors.New("exit status 113")},
	}}
	m := newManager(t, f)
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q"}))
	require.NoError(t, m.Uninstall(t.Context()))
	_, err := os.Stat(m.PlistPath())
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, m.Uninstall(t.Context()))
}

func TestRestart(t *testing.T) {
	f := &fakeRunner{}
	m := newManager(t, f)
	require.NoError(t, m.Restart(t.Context()))
	require.Equal(t, []string{"kickstart -k gui/501/dev.quacktel.test"}, f.calls)
}

const printSample = `gui/501/dev.quacktel.serve = {
	active count = 1
	path = /Users/example/Library/LaunchAgents/dev.quacktel.serve.plist
	type = LaunchAgent
	state = running

	program = /usr/local/bin/quacktel
	arguments = {
		/usr/local/bin/quacktel
		serve
	}

	stdout path = /Users/example/Library/Logs/quacktel/launchd.log
	working directory = /
	runs = 3
	pid = 1234
	immediate reason = inefficient
	forks = 0
	execs = 1
	last exit code = 0

	properties = keepalive | runatload
}
`

func TestStatus(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{"print": {out: printSample}}}
	m := newManager(t, f)
	data, err := Render(Plist{Label: m.Label, ProgramArguments: []string{"/bin/q", "serve"}, LogPath: "/tmp/l"})
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(m.PlistPath()), 0o755))
	require.NoError(t, os.WriteFile(m.PlistPath(), data, 0o600))

	st, err := m.Status(t.Context())
	require.NoError(t, err)
	require.True(t, st.Loaded)
	require.Equal(t, "running", st.State)
	require.Equal(t, 1234, st.PID)
	require.NotNil(t, st.LastExit)
	require.Equal(t, 0, *st.LastExit)
	require.Equal(t, []string{"/bin/q", "serve"}, st.Args)
}

func TestStatusNotLoaded(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{
		"print": {out: `Could not find service "dev.quacktel.test" in domain for user gui: 501`, err: errors.New("exit status 113")},
	}}
	m := newManager(t, f)

	st, err := m.Status(t.Context())
	require.NoError(t, err)
	require.False(t, st.Loaded)
	require.Nil(t, st.Args)
}

func TestStatusOtherErrorFails(t *testing.T) {
	f := &fakeRunner{reply: map[string]fakeReply{"print": {out: "boom", err: errors.New("exit status 1")}}}
	_, err := newManager(t, f).Status(t.Context())
	require.ErrorContains(t, err, "boom")
}
