package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type doctorCheck struct {
	Name, Status, Detail string
}

func runDoctor(t *testing.T, args ...string) (stdout string, code int) {
	t.Helper()
	out, stderr, err := runBinHome(t, t.TempDir(), append([]string{"doctor"}, args...)...)
	var ee *exec.ExitError
	switch {
	case err == nil:
		code = 0
	case errors.As(err, &ee):
		code = ee.ExitCode()
	default:
		require.NoError(t, err)
	}
	require.Empty(t, stderr, "exit-1 path must not log a second error")

	return out, code
}

func byName(t *testing.T, out string) map[string]doctorCheck {
	t.Helper()
	var checks []doctorCheck
	require.NoError(t, json.Unmarshal([]byte(out), &checks), out)
	m := map[string]doctorCheck{}
	for _, c := range checks {
		m[c.Name] = c
	}

	return m
}

func TestDoctorEndToEnd(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)
	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir, "--auth")
	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture, "Authorization", "Bearer "+readToken(t, dir)))

	var checks map[string]doctorCheck
	deadline := time.Now().Add(10 * time.Second)
	for {
		out, code := runDoctor(t, "--data-dir", dir, "--listen", addr, "--json")
		checks = byName(t, out)
		if code == 0 && regexp.MustCompile(`logs=[1-9]`).MatchString(checks["read_path"].Detail) {
			break
		}
		require.True(t, time.Now().Before(deadline), "doctor never reported logs: %s", out)
		time.Sleep(100 * time.Millisecond)
	}
	for _, c := range checks {
		require.Contains(t, []string{"ok", "warn"}, c.Status, "%s: %s", c.Name, c.Detail)
	}
	require.Equal(t, "ok", checks["receiver"].Status)
	require.Equal(t, "ok", checks["serve_lock"].Status)
	require.Equal(t, "ok", checks["auth_headers"].Status)

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	out, code := runDoctor(t, "--data-dir", dir, "--listen", addr, "--json")
	require.Equal(t, 1, code)
	checks = byName(t, out)
	require.Equal(t, "fail", checks["serve_lock"].Status)
	require.Equal(t, "fail", checks["receiver"].Status)

	out, code = runDoctor(t, "--data-dir", dir, "--listen", addr)
	require.Equal(t, 1, code)
	require.Regexp(t, `(?m)^STATUS +CHECK +DETAIL$`, out)
	require.Regexp(t, `(?m)^fail +serve_lock `, out)
}

func TestDoctorReadsRecordedAgentArgs(t *testing.T) {
	home := t.TempDir()
	dir := dataDir(t)
	label := randomLabel(t)
	plist := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
	require.NoError(t, os.MkdirAll(filepath.Dir(plist), 0o755))
	body := `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>Label</key><string>` + label + `</string>
<key>ProgramArguments</key><array><string>/nonexistent/quacktel</string><string>serve</string><string>--auth</string><string>--data-dir=` + dir + `</string><string>--listen</string><string>127.0.0.1:1</string></array>
</dict></plist>`
	require.NoError(t, os.WriteFile(plist, []byte(body), 0o644))

	out, _, err := runBinHome(t, home, "doctor", "--label", label, "--json")
	var ee *exec.ExitError
	require.ErrorAs(t, err, &ee)
	checks := byName(t, out)
	require.Contains(t, checks["data_dir"].Detail, dir)
	require.Contains(t, checks["receiver"].Detail, "127.0.0.1:1")
	require.Equal(t, "fail", checks["service"].Status)
}

func TestWithDataDir(t *testing.T) {
	require.Equal(t, []string{"--auth", "--data-dir", "/abs/d", "--listen", "x"},
		withDataDir([]string{"--auth", "--data-dir", "rel/d", "--listen", "x"}, "/abs/d"))
	require.Equal(t, []string{"--data-dir=/abs/d"}, withDataDir([]string{"--data-dir=rel/d"}, "/abs/d"))

	in := []string{"--auth"}
	require.Equal(t, in, withDataDir(in, "/abs/d"))
}

func TestRelativeDataDirIsRecordedAbsolute(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	for _, args := range [][]string{{"--data-dir", "rel/d"}, {"--data-dir=rel/d"}} {
		probe, err := parseServeArgs(args)
		require.NoError(t, err)
		require.Equal(t, filepath.Join(cwd, "rel", "d"), probe.DataDir)

		recorded := withDataDir(args, probe.DataDir)
		require.Contains(t, recorded[len(recorded)-1], filepath.Join(cwd, "rel", "d"))
		again, err := parseServeArgs(recorded)
		require.NoError(t, err)
		require.Equal(t, probe.DataDir, again.DataDir)
	}
}
