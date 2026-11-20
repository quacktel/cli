package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/quacktel/cli/internal/manifest"
)

// fixtureWindow reaches the log fixture's fixed 2026-05 timestamps, which the 24h default would miss.
const fixtureWindow = "87600h"

// seedMCPData stores the Claude Code log fixture and the Go example app's three spans.
func seedMCPData(t *testing.T) string {
	t.Helper()
	fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "claude_code_logs.json"))
	require.NoError(t, err)

	addr := freeAddr(t)
	dir := dataDir(t)
	p := startServe(t, addr, dir)
	require.Equal(t, http.StatusOK, post(t, "http://"+addr+"/v1/logs", "application/json", fixture))
	runGoApp(t, addr)
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	return dir
}

func connectMCP(t *testing.T, dir string, extra ...string) (*mcp.ClientSession, *syncBuf) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), binPath, append([]string{"mcp", "--data-dir", dir}, extra...)...)
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	stderr := &syncBuf{}
	cmd.Stderr = stderr

	client := mcp.NewClient(&mcp.Implementation{Name: "quacktel-test", Version: "0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	return session, stderr
}

func callTool(t *testing.T, s *mcp.ClientSession, name string, args map[string]any) (*mcp.CallToolResult, map[string]any) {
	t.Helper()
	res, err := s.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)
	structured, _ := res.StructuredContent.(map[string]any)

	return res, structured
}

// okRows calls a rows-returning tool, requires success and the untrusted note, and returns the rows.
func okRows(t *testing.T, s *mcp.ClientSession, name string, args map[string]any) []map[string]any {
	t.Helper()
	res, out := callTool(t, s, name, args)
	require.False(t, res.IsError, "%s: %+v", name, res.Content)
	require.Contains(t, out["note"], "untrusted")

	rows, _ := out["rows"].([]any)
	got := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		got = append(got, r.(map[string]any))
	}

	return got
}

func toolNames(t *testing.T, s *mcp.ClientSession) []string {
	t.Helper()
	list, err := s.ListTools(t.Context(), nil)
	require.NoError(t, err)
	names := make([]string, 0, len(list.Tools))
	for _, tool := range list.Tools {
		names = append(names, tool.Name)
		require.NotNil(t, tool.Annotations, tool.Name)
		require.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
	}

	return names
}

func TestMCPEndToEnd(t *testing.T) {
	dir := seedMCPData(t)
	s, stderr := connectMCP(t, dir, "--debug")

	require.ElementsMatch(t, []string{"list_services", "describe_schema", "search_logs", "find_spans", "get_trace", "sql"}, toolNames(t, s))

	var services []string
	for _, r := range okRows(t, s, "list_services", map[string]any{"since": fixtureWindow}) {
		services = append(services, r["service"].(string))
	}
	require.Contains(t, services, "claude-code")
	require.Contains(t, services, "otel-go-app")

	res, schema := callTool(t, s, "describe_schema", nil)
	require.False(t, res.IsError, "%+v", res.Content)
	require.Contains(t, schema["note"], "untrusted")

	logs := okRows(t, s, "search_logs", map[string]any{"event_name": "api_request", "since": fixtureWindow})
	require.NotEmpty(t, logs)
	require.Equal(t, "[REDACTED]", logs[0]["attrs"].(map[string]any)["api_key"])

	spans := okRows(t, s, "find_spans", map[string]any{"service": "otel-go-app", "status": "error"})
	require.Len(t, spans, 1)
	require.Equal(t, "load-cache", spans[0]["name"])

	res, trace := callTool(t, s, "get_trace", map[string]any{"trace_id": spans[0]["trace_id"]})
	require.False(t, res.IsError, "%+v", res.Content)
	require.Contains(t, trace["note"], "untrusted")
	depths := map[string]float64{}
	traceSpans, _ := trace["spans"].([]any)
	require.Len(t, traceSpans, 3)
	for _, sp := range traceSpans {
		m := sp.(map[string]any)
		depths[m["name"].(string)] = m["depth"].(float64)
	}
	require.Equal(t, map[string]float64{"GET /items": 0, "chat test-model": 1, "load-cache": 1}, depths)

	counts := okRows(t, s, "sql", map[string]any{"query": "SELECT count(*) AS n FROM spans"})
	require.Len(t, counts, 1)
	require.EqualValues(t, 3, counts[0]["n"])

	res, _ = callTool(t, s, "sql", map[string]any{"query": "COPY (SELECT 1) TO '/tmp/x.csv'"})
	require.True(t, res.IsError)

	require.NoError(t, s.Close())
	// Stdout carried only protocol, so diagnostics must have gone to stderr.
	require.NotEmpty(t, stderr.String())
}

func TestMCPScoped(t *testing.T) {
	dir := seedMCPData(t)
	s, _ := connectMCP(t, dir, "--service", "otel-go-app")

	rows := okRows(t, s, "list_services", nil)
	require.Len(t, rows, 1)
	require.Equal(t, "otel-go-app", rows[0]["service"])

	res, _ := callTool(t, s, "search_logs", map[string]any{"service": "claude-code"})
	require.True(t, res.IsError)

	files := readManifest(t, filepath.Join(dir, "services", manifest.ServiceDir("claude-code")))
	require.NotEmpty(t, files)
	path := files[0].Path
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, "services", manifest.ServiceDir("claude-code"), path)
	}
	require.FileExists(t, path)
	res, _ = callTool(t, s, "sql", map[string]any{"query": "SELECT * FROM read_parquet('" + path + "')"})
	require.True(t, res.IsError)

	counts := okRows(t, s, "sql", map[string]any{"query": "SELECT count(*) AS n FROM logs"})
	require.Len(t, counts, 1)
	require.EqualValues(t, 0, counts[0]["n"])
}

func TestMCPNoSQL(t *testing.T) {
	s, _ := connectMCP(t, dataDir(t), "--no-sql")

	names := toolNames(t, s)
	require.Len(t, names, 5)
	require.NotContains(t, names, "sql")
}

func TestMCPSQLCannotReadHeadersFile(t *testing.T) {
	dir := dataDir(t)
	p := startServe(t, freeAddr(t), dir, "--auth")
	token := readToken(t, dir)
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, p.wait(t), p.stderr.String())

	s, _ := connectMCP(t, dir)
	res, _ := callTool(t, s, "sql", map[string]any{"query": "SELECT content FROM read_text('" + filepath.Join(dir, "otlp-headers") + "')"})
	require.True(t, res.IsError)
	raw, err := json.Marshal(res)
	require.NoError(t, err)
	require.NotContains(t, string(raw), token)
	require.NotContains(t, string(raw), "Bearer")
}
