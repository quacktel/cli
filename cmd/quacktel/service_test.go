package main

import (
	"bytes"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/require"
)

func randomLabel(t *testing.T) string {
	t.Helper()

	return "dev.quacktel.test-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func runBinHome(t *testing.T, home string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Env = append(os.Environ(), "HOME="+home)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err = cmd.Run()

	return out.String(), errb.String(), err
}

func TestServiceInstallRejectsBadArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"zero retention", []string{"--retention", "0s"}, "--retention must be greater than 0"},
		{"unknown flag", []string{"--bogus"}, "unknown flag"},
		{"non-loopback", []string{"--listen", "0.0.0.0:4318"}, "non-loopback"},
		{"help is not a flag", []string{"--help"}, "unknown flag"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			args := append([]string{"service", "install", "--label", randomLabel(t), "--"}, tt.args...)
			_, stderr, err := runBinHome(t, home, args...)
			require.Error(t, err)
			require.Contains(t, stderr, tt.want)

			require.NoError(t, filepath.WalkDir(home, func(path string, _ fs.DirEntry, err error) error {
				require.NoError(t, err)
				require.False(t, strings.HasSuffix(path, ".plist"), "unexpected plist %s", path)

				return nil
			}))
			_, err = os.Stat(filepath.Join(home, "Library", "LaunchAgents"))
			require.ErrorIs(t, err, fs.ErrNotExist)
		})
	}
}

func TestValidateServeArgsAccepts(t *testing.T) {
	require.NoError(t, validateServeArgs(nil))
	require.NoError(t, validateServeArgs([]string{"--auth", "--listen", "127.0.0.1:4319", "--retention", "24h"}))
	require.NoError(t, validateServeArgs([]string{"--listen", "0.0.0.0:4318", "--unsafe-bind"}))
}

func TestServiceInstallPassthroughArgs(t *testing.T) {
	var args cli
	parser, err := kong.New(&args, kongVars())
	require.NoError(t, err)
	_, err = parser.Parse([]string{"service", "install", "--label", "x", "--", "--retention", "24h"})
	require.NoError(t, err)
	require.Equal(t, []string{"--", "--retention", "24h"}, args.Service.Install.Args)
}

func TestShellJoin(t *testing.T) {
	require.Equal(t, `/bin/q --data-dir '/a b' 'it'\''s'`, shellJoin([]string{"/bin/q", "--data-dir", "/a b", "it's"}))
}

func TestServiceLaunchdReal(t *testing.T) {
	if os.Getenv("QUACKTEL_TEST_LAUNCHD") != "1" {
		t.Skip("set QUACKTEL_TEST_LAUNCHD=1 to run against the real launchd")
	}
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	label := randomLabel(t)
	plist := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
	t.Cleanup(func() { _, _, _ = runBinHome(t, home, "service", "uninstall", "--label", label) })

	addr := freeAddr(t)
	dir := dataDir(t)
	stdout, stderr, err := runBinHome(t, home, "service", "install", "--label", label, "--", "--data-dir", dir, "--listen", addr)
	require.NoError(t, err, stderr)
	require.Contains(t, stdout, plist)

	dial := func() bool {
		conn, err := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(t.Context(), "tcp", addr)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}
	deadline := time.Now().Add(15 * time.Second)
	for !dial() {
		require.True(t, time.Now().Before(deadline), "service did not start listening on %s", addr)
		time.Sleep(100 * time.Millisecond)
	}

	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)
	headers, err := os.ReadFile(filepath.Join(dir, otlpHeadersFile))
	require.NoError(t, err)
	auth := strings.TrimPrefix(strings.TrimSpace(string(headers)), "authorization=")
	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture, "Authorization", auth))

	_, stderr, err = runBinHome(t, home, "service", "uninstall", "--label", label)
	require.NoError(t, err, stderr)
	_, err = os.Stat(plist)
	require.ErrorIs(t, err, fs.ErrNotExist)

	deadline = time.Now().Add(10 * time.Second)
	for dial() {
		require.True(t, time.Now().Before(deadline), "port %s still open after uninstall", addr)
		time.Sleep(100 * time.Millisecond)
	}
}
