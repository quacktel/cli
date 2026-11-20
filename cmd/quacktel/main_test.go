package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
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

func TestQueryLogsViewEmptyDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	stdout, err := runBin(t, "query", "--data-dir", dir, "--format", "json", "SELECT count(*) AS n FROM logs")
	require.NoError(t, err)
	require.Equal(t, "[{\"n\":0}]\n", stdout)
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

	q := "SELECT event_name, session_id, attrs['api_key'] AS k, attrs['input_tokens'] AS it, attrs['cost_usd'] AS cost, trace_id FROM logs ORDER BY time, event_name"
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

func readToken(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "otlp-headers"))
	require.NoError(t, err)
	m := regexp.MustCompile(`^authorization=Bearer ([0-9a-f]{64})\n$`).FindStringSubmatch(string(raw))
	require.NotNil(t, m, string(raw))

	return m[1]
}

func TestServeAuth(t *testing.T) {
	dir := dataDir(t)
	addr := freeAddr(t)
	url := "http://" + addr + "/v1/logs"
	body := []byte("{}")

	p := startServe(t, addr, dir, "--auth")
	info, err := os.Stat(filepath.Join(dir, "otlp-headers"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	token := readToken(t, dir)
	require.NotContains(t, p.stderr.String(), token)
	require.Contains(t, p.stderr.String(), "otlp-headers")

	require.Equal(t, http.StatusUnauthorized, post(t, url, "application/json", body))
	require.Equal(t, http.StatusOK, post(t, url, "application/json", body, "Authorization", "Bearer "+token))
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	raw, err := os.ReadFile(filepath.Join(dir, "stats.json"))
	require.NoError(t, err)
	var stats map[string]any
	require.NoError(t, json.Unmarshal(raw, &stats))
	require.EqualValues(t, 1, stats["unauthorized"])

	p = startServe(t, addr, dir, "--auth")
	require.Equal(t, token, readToken(t, dir))
	require.Equal(t, http.StatusOK, post(t, url, "application/json", body, "Authorization", "Bearer "+token))
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())
}

func TestServeUnsafeBindImpliesAuth(t *testing.T) {
	dir := dataDir(t)
	addr := freeAddr(t)
	p := startServe(t, addr, dir, "--unsafe-bind")
	require.Equal(t, http.StatusUnauthorized, post(t, "http://"+addr+"/v1/logs", "application/json", []byte("{}")))
	require.FileExists(t, filepath.Join(dir, "otlp-headers"))
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())
}

func TestServeRejectsBadHeadersFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		mode    os.FileMode
	}{
		{"corrupted", "authorization=Bearer short\n", 0o600},
		{"wide mode", "authorization=Bearer " + strings.Repeat("a", 64) + "\n", 0o644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := dataDir(t)
			path := filepath.Join(dir, "otlp-headers")
			require.NoError(t, os.WriteFile(path, []byte(tt.content), tt.mode))
			require.NoError(t, os.Chmod(path, tt.mode))
			_, err := runBin(t, "serve", "--auth", "--listen", "127.0.0.1:0", "--data-dir", dir)
			require.Error(t, err)
		})
	}
}

func TestServeOpenShellOCSF(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "openshell_ocsf_logs.json"))
	require.NoError(t, err)
	req := plogotlp.NewExportRequest()
	require.NoError(t, req.UnmarshalJSON(fixture))
	pb, err := req.MarshalProto()
	require.NoError(t, err)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err = zw.Write(pb)
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir, "--auth")
	auth := "Bearer " + readToken(t, dir)
	// The second post simulates a gateway retry.
	for range 2 {
		require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/x-protobuf", buf.Bytes(),
			"Content-Encoding", "gzip", "Authorization", auth))
	}
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	from := " FROM logs WHERE "
	query := func(sql string) []map[string]any {
		out, err := runBin(t, "query", "--data-dir", dir, "--format", "json", sql)
		require.NoError(t, err)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal([]byte(out), &rows))

		return rows
	}

	// Duplicates are stored: the store does not dedupe gateway retries.
	rows := query("SELECT count(*) AS n" + from + "service_name='openshell'")
	require.EqualValues(t, 6, rows[0]["n"])
	rows = query("SELECT count(DISTINCT attrs['ocsf.metadata.uid']) AS n" + from + "service_name='openshell'")
	require.EqualValues(t, 2, rows[0]["n"])

	rows = query( // DISTINCT collapses the duplicate retry rows, as a real query would.
		"SELECT DISTINCT json_extract_string(body, '$.action') AS action, attrs['openshell.sandbox.id'] AS sb, event_name, attrs['ocsf.metadata.uid'] AS uid" +
			from + "attrs['ocsf.class_uid'] = '4001' ORDER BY attrs['ocsf.metadata.uid'] LIMIT 2")
	require.Len(t, rows, 2)
	require.Equal(t, "Allowed", rows[0]["action"])
	require.Equal(t, "Denied", rows[1]["action"])
	for _, r := range rows {
		require.Equal(t, "sb-test", r["sb"])
	}

	rows = query("SELECT event_name, attrs['openshell.ocsf.gap.dropped'] AS dropped, resource_attrs['openshell.sandbox.id'] AS sb" +
		from + "event_name = 'openshell.ocsf.gap' LIMIT 1")
	require.Len(t, rows, 1)
	require.Equal(t, "openshell.ocsf.gap", rows[0]["event_name"])
	require.Equal(t, "7", rows[0]["dropped"])
	require.Equal(t, "sb-test", rows[0]["sb"])
}

func queryRows(t *testing.T, dir, sql string) []map[string]any {
	t.Helper()
	out, err := runBin(t, "query", "--data-dir", dir, "--format", "json", sql)
	require.NoError(t, err)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &rows))

	return rows
}

func readStats(t *testing.T, dir string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "stats.json"))
	require.NoError(t, err)
	var stats map[string]any
	require.NoError(t, json.Unmarshal(raw, &stats))

	return stats
}

func TestServeTracesFromGoApp(t *testing.T) {
	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir)

	appBin := filepath.Join(t.TempDir(), "app")
	build := exec.CommandContext(t.Context(), "go", "build", "-C", filepath.Join("..", "..", "examples", "otel-go-app"), "-o", appBin, ".")
	// The example is built without the race detector, which the test run's GOFLAGS may request.
	build.Env = append(os.Environ(), "GOFLAGS="+strings.Join(slices.DeleteFunc(strings.Fields(os.Getenv("GOFLAGS")), func(f string) bool {
		return f == "-race"
	}), " "))
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))

	app := exec.CommandContext(t.Context(), appBin)
	app.Env = append(os.Environ(),
		"OTEL_EXPORTER_OTLP_ENDPOINT=http://"+addr,
		"OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf",
		"HOME="+t.TempDir())
	out, err = app.CombinedOutput()
	require.NoError(t, err, string(out))

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	rows := queryRows(t, dir, "SELECT trace_id, span_id, parent_span_id, name, kind, status_code, http_request_method, http_route, "+
		"http_response_status_code, gen_ai_request_model, gen_ai_usage_input_tokens, gen_ai_usage_output_tokens, session_id, "+
		"attrs['auth_token'] AS tok, len(events) AS n_events, events[1].name AS ev, duration_ns "+
		"FROM spans ORDER BY name")
	require.Len(t, rows, 3)
	// Byte order sorts the uppercase root first.
	root, chat, cache := rows[0], rows[1], rows[2]
	require.Equal(t, "GET /items", root["name"])
	require.Equal(t, "chat test-model", chat["name"])
	require.Equal(t, "load-cache", cache["name"])
	for _, r := range rows {
		require.Equal(t, root["trace_id"], r["trace_id"])
		require.GreaterOrEqual(t, r["duration_ns"], float64(0))
	}

	require.Nil(t, root["parent_span_id"])
	require.Equal(t, root["span_id"], chat["parent_span_id"])
	require.Equal(t, root["span_id"], cache["parent_span_id"])

	require.Equal(t, "server", root["kind"])
	require.Equal(t, "client", chat["kind"])
	require.Equal(t, "internal", cache["kind"])

	require.Equal(t, "GET", root["http_request_method"])
	require.Equal(t, "/items", root["http_route"])
	require.EqualValues(t, 200, root["http_response_status_code"])

	require.Equal(t, "test-model", chat["gen_ai_request_model"])
	require.EqualValues(t, 12, chat["gen_ai_usage_input_tokens"])
	require.EqualValues(t, 34, chat["gen_ai_usage_output_tokens"])
	require.Equal(t, "sess-go-1", chat["session_id"])
	require.Equal(t, "[REDACTED]", chat["tok"])
	require.Equal(t, "retry", chat["ev"])

	require.Equal(t, "error", cache["status_code"])
	require.Equal(t, "exception", cache["ev"])

	require.EqualValues(t, 3, readStats(t, dir)["spans_accepted"])

	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(b), "tok-not-real", path)

		return nil
	}))
}

func TestServeTracesJSON(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "traces.json"))
	require.NoError(t, err)

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir)
	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/traces", "application/json", fixture))
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	rows := queryRows(t, dir, "SELECT name, kind, status_code, http_response_status_code, service_name "+
		"FROM spans ORDER BY name")
	require.Len(t, rows, 2)
	require.Equal(t, "GET /missing", rows[0]["name"])
	require.Equal(t, "server", rows[0]["kind"])
	require.Equal(t, "error", rows[0]["status_code"])
	require.EqualValues(t, 404, rows[0]["http_response_status_code"])
	require.Equal(t, "json-svc", rows[0]["service_name"])
	require.Equal(t, "lookup", rows[1]["name"])
	require.Equal(t, "internal", rows[1]["kind"])
	require.Equal(t, "ok", rows[1]["status_code"])
}

func TestServeRejectsBadMaintenanceFlags(t *testing.T) {
	for _, args := range [][]string{{"--seal-min-files", "1"}, {"--retention", "0s"}, {"--max-disk", "0"}, {"--compact-interval", "0s"}, {"--retire-grace", "0s"}} {
		t.Run(args[0]+"="+args[1], func(t *testing.T) {
			dir := dataDir(t)
			cmd := exec.CommandContext(t.Context(), binPath, append([]string{"serve", "--listen", "127.0.0.1:0", "--data-dir", dir}, args...)...)
			cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
			out, err := cmd.CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(out), args[0])
			require.NoFileExists(t, filepath.Join(dir, ".lock"))
		})
	}
}

func countParquet(t *testing.T, root string) int {
	t.Helper()
	n := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			n++
		}

		return nil
	}))

	return n
}

func TestServeCompaction(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)
	req := plogotlp.NewExportRequest()
	require.NoError(t, req.UnmarshalJSON(fixture))
	perPost := req.Logs().LogRecordCount()
	const posts = 12

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir, "--flush-interval", "50ms", "--compact-interval", "200ms", "--seal-min-files", "3", "--retire-grace", "200ms")

	var svcDir string
	for i := range posts {
		before := 0
		if svcDir != "" {
			before = countParquet(t, svcDir)
		}
		require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture))
		if i == 0 {
			require.Eventually(t, func() bool {
				dirs, _ := filepath.Glob(filepath.Join(dir, "services", "*"))
				if len(dirs) != 1 {
					return false
				}
				svcDir = dirs[0]

				return countParquet(t, svcDir) > 0
			}, 5*time.Second, 10*time.Millisecond)

			continue
		}
		// A file only appears once the flush ran, so the next post lands in a later flush.
		require.Eventually(t, func() bool { return countParquet(t, svcDir) != before }, 5*time.Second, 10*time.Millisecond)
	}

	// A pass leaves at most SealMinFiles sealed files plus fewer than SealMinFiles tails, and a few more tails can land before the next pass.
	logsDir := filepath.Join(svcDir, "logs")
	require.Eventually(t, func() bool {
		n := countParquet(t, logsDir)

		return n <= 2*3+1 && n == len(readManifest(t, svcDir)) && readStats(t, dir)["compactions"].(float64) >= 1
	}, 10*time.Second, 50*time.Millisecond, p.stderr.String())

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	want := posts * perPost
	rows := queryRows(t, dir, "SELECT count(*) AS n FROM logs")
	require.EqualValues(t, want, rows[0]["n"])

	files := readManifest(t, svcDir)
	listed := map[string]bool{}
	var sum int64
	for _, f := range files {
		listed[filepath.Join(svcDir, f.Path)] = true
		sum += f.Rows
	}
	require.EqualValues(t, want, sum)

	require.NoError(t, filepath.WalkDir(svcDir, func(path string, d fs.DirEntry, err error) error {
		require.NoError(t, err)
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			require.True(t, listed[path], "unlisted parquet file %s", path)
		}

		return nil
	}))
}

func TestServeStatsWrittenOnChange(t *testing.T) {
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir, "--flush-interval", "50ms")
	statsPath := filepath.Join(dir, "stats.json")
	mtime := func() time.Time {
		info, err := os.Stat(statsPath)
		require.NoError(t, err)

		return info.ModTime()
	}

	require.Eventually(t, func() bool { _, err := os.Stat(statsPath); return err == nil }, 5*time.Second, 10*time.Millisecond)
	initial := mtime()
	time.Sleep(250 * time.Millisecond)
	require.Equal(t, initial, mtime())

	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture))
	require.Eventually(t, func() bool { return !mtime().Equal(initial) }, 5*time.Second, 20*time.Millisecond)

	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())
}

type manifestFile struct {
	Path string `json:"path"`
	Rows int64  `json:"rows"`
}

func readManifest(t *testing.T, svcDir string) []manifestFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(svcDir, "manifest.json"))
	require.NoError(t, err)
	var m struct {
		Files []manifestFile `json:"files"`
	}
	require.NoError(t, json.Unmarshal(raw, &m))

	return m.Files
}

func TestQueryServiceScope(t *testing.T) {
	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir)
	for _, name := range []string{"claude_code_logs.json", "openshell_ocsf_logs.json"} {
		fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture))
	}
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	all := queryRows(t, dir, "SELECT DISTINCT service_name FROM logs ORDER BY service_name")
	require.Len(t, all, 2)

	out, err := runBin(t, "query", "--data-dir", dir, "--service", "claude-code", "--format", "json", "SELECT DISTINCT service_name FROM logs")
	require.NoError(t, err)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &rows))
	require.Equal(t, []map[string]any{{"service_name": "claude-code"}}, rows)

	out, err = runBin(t, "query", "--data-dir", dir, "--service", "nonexistent", "--format", "json", "SELECT count(*) AS n FROM logs")
	require.NoError(t, err)
	require.JSONEq(t, `[{"n":0}]`, out)
}

func TestQueryTableNullIsEmpty(t *testing.T) {
	dir := dataDir(t)
	out, err := runBin(t, "query", "--data-dir", dir, "--format", "table", "SELECT 1 AS a, NULL AS b, 'x' AS c")
	require.NoError(t, err)
	require.NotContains(t, out, "<nil>")
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	require.Len(t, lines, 2)
	require.Equal(t, []string{"1", "x"}, strings.Fields(lines[1]))
}

func TestMCPMissingDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "absent")
	cmd := exec.CommandContext(t.Context(), binPath, "mcp", "--data-dir", dir)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Empty(t, stdout.String())
	require.Contains(t, stderr.String(), "run quacktel serve first")
	require.NoDirExists(t, dir)
}
