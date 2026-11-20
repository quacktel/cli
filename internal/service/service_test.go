package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeRunner struct {
	mu    sync.Mutex
	calls []string
	// reply maps a launchctl verb to its canned output and error.
	reply map[string]fakeReply
}

type fakeReply struct {
	out string
	err error
}

func (f *fakeRunner) run(_ context.Context, args ...string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.Join(args, " "))
	r := f.reply[args[0]]

	return []byte(r.out), r.err
}

func newManager(t *testing.T, f *fakeRunner) *Manager {
	t.Helper()

	return &Manager{Label: "dev.quacktel.test", Home: t.TempDir(), UID: 501, Run: f.run}
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
	require.Contains(t, out, "<key>SuccessfulExit</key>\n\t\t\t<false></false>")

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
		"bootstrap gui/501 " + m.PlistPath(),
	}, f.calls)

	// A second install replaces the plist and reloads.
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q", "serve", "--retention", "24h"}))
	p, err := ReadPlist(m.PlistPath())
	require.NoError(t, err)
	require.Equal(t, []string{"/bin/q", "serve", "--retention", "24h"}, p.ProgramArguments)
	require.Len(t, f.calls, 4)
	require.Equal(t, "bootout gui/501/dev.quacktel.test", f.calls[2])

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
	require.NoError(t, m.Install(t.Context(), []string{"/bin/q", "serve"}))

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
