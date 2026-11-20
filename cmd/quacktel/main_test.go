package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

var binPath string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "quacktel-test")
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to create temp dir:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	binPath = filepath.Join(dir, "quacktel")
	if out, err := exec.Command("go", "build", "-o", binPath, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build binary: %v\n%s", err, out)
		return 1
	}

	return m.Run()
}

func runBin(t *testing.T, args ...string) (string, error) {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	err := cmd.Run()

	return stdout.String(), err
}

func TestVersion(t *testing.T) {
	stdout, err := runBin(t, "version")
	require.NoError(t, err)
	require.Regexp(t, `^quacktel \S+ \(commit \S+, go[^)]+\)\n$`, stdout)
}

func TestHelp(t *testing.T) {
	stdout, err := runBin(t, "--help")
	require.NoError(t, err)
	require.Contains(t, stdout, "version")
}

func TestQueryJSON(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	stdout, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "SELECT 42 AS answer")
	require.NoError(t, err)
	require.Equal(t, "[{\"answer\":42}]\n", stdout)
}

func TestQueryRejectsCopy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	out := filepath.Join(dir, "x.csv")
	_, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "COPY (SELECT 1) TO '"+out+"'")
	require.Error(t, err)
	require.NoFileExists(t, out)
}

func TestQueryRejectsWideDataDir(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "SELECT 1")
	require.Error(t, err)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	return addr
}

func dataDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "d")
	require.NoError(t, os.Mkdir(dir, 0o700))

	return dir
}

// syncBuf lets tests read stderr while the process is still writing it.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.b.String()
}

type serveProc struct {
	cmd    *exec.Cmd
	stderr *syncBuf
	done   chan error
}

func (p *serveProc) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.done:
		return err
	case <-time.After(10 * time.Second):
		require.FailNow(t, "serve did not exit", p.stderr.String())
	}

	return nil
}

func startServe(t *testing.T, addr, dir string, extra ...string) *serveProc {
	t.Helper()
	args := append([]string{"serve", "--listen", addr, "--data-dir", dir, "--flush-interval", "100ms"}, extra...)
	cmd := exec.CommandContext(t.Context(), binPath, args...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	p := &serveProc{cmd: cmd, stderr: &syncBuf{}, done: make(chan error, 1)}
	cmd.Stderr = p.stderr
	require.NoError(t, cmd.Start())
	go func() { p.done <- cmd.Wait() }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(t.Context(), "tcp", addr)
		if err == nil {
			require.NoError(t, conn.Close())
			return p
		}
		select {
		case err := <-p.done:
			require.FailNow(t, "serve exited early", "%v\n%s", err, p.stderr.String())
		case <-time.After(20 * time.Millisecond):
		}
		require.True(t, time.Now().Before(deadline), "serve did not start: %s", p.stderr.String())
	}
}

func post(t *testing.T, url, ctype string, body []byte, hdr ...string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", ctype)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

func TestServeEndToEnd(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)
	req := plogotlp.NewExportRequest()
	require.NoError(t, req.UnmarshalJSON(fixture))
	pb, err := req.MarshalProto()
	require.NoError(t, err)

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir)
	base := "http://" + addr

	require.Equal(t, http.StatusOK, post(t, base+"/v1/logs", "application/json", fixture))
	require.Equal(t, http.StatusOK, post(t, base+"/v1/logs", "application/x-protobuf", pb))
	require.Equal(t, http.StatusForbidden, post(t, base+"/v1/logs", "application/json", fixture, "Origin", "https://evil.example"))
	require.Equal(t, http.StatusOK, post(t, base+"/v1/metrics", "application/x-protobuf", []byte("ignored")))

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	q := "SELECT event_name, session_id, attrs['api_key'] AS k, attrs['input_tokens'] AS it, attrs['cost_usd'] AS cost, trace_id FROM read_parquet('" + dir +
		"/services/*/logs/**/*.parquet') ORDER BY time, event_name"
	out, err := runBin(t, "query", "--data-dir", dir, "--format", "json", q)
	require.NoError(t, err)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &rows))
	require.Len(t, rows, 4)
	traceID := "0123456789abcdef0123456789abcdef"
	for i, row := range rows {
		require.Equal(t, "sess-test-1", row["session_id"])
		// Both posts carry the same timestamps, so rows sort as two prompts then two requests.
		if i < 2 {
			require.Equal(t, "claude_code.user_prompt", row["event_name"])
			require.Nil(t, row["k"])
		} else {
			require.Equal(t, "claude_code.api_request", row["event_name"])
			require.Equal(t, "[REDACTED]", row["k"])
			require.Equal(t, "100", row["it"])
			require.Equal(t, "0.0123", row["cost"])
			require.Equal(t, traceID, row["trace_id"])
		}
	}

	raw, err := os.ReadFile(filepath.Join(dir, "stats.json"))
	require.NoError(t, err)
	var stats map[string]any
	require.NoError(t, json.Unmarshal(raw, &stats))
	require.EqualValues(t, 1, stats["metrics_dropped"])
	require.EqualValues(t, 2, stats["values_redacted"])
	require.EqualValues(t, 4, stats["records_accepted"])

	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(b), "sk-test-not-a-real-key", path)

		return nil
	}))
}

func TestServeMaxInFlightFlag(t *testing.T) {
	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir, "--max-inflight", "1")

	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/metrics", "application/x-protobuf", []byte("ignored")))

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())
}

func TestServeRefusesNonLoopback(t *testing.T) {
	_, err := runBin(t, "serve", "--listen", "0.0.0.0:0", "--data-dir", dataDir(t))
	require.Error(t, err)
}

func TestServeRefusesSecondInstance(t *testing.T) {
	dir := dataDir(t)
	p := startServe(t, freeAddr(t), dir)

	cmd := exec.CommandContext(t.Context(), binPath, "serve", "--listen", freeAddr(t), "--data-dir", dir)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	out, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "another quacktel serve is using data dir")

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t))
}

func TestServeRejectsLooseDataDir(t *testing.T) {
	dir := dataDir(t)
	require.NoError(t, os.Chmod(dir, 0o755))
	_, err := runBin(t, "serve", "--listen", "127.0.0.1:0", "--data-dir", dir)
	require.Error(t, err)
}
