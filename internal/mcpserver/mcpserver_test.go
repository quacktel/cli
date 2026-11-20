package mcpserver_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/mcpserver"
	"github.com/quacktel/cli/internal/tools"
	"github.com/quacktel/cli/internal/tools/toolstest"
)

func connect(t *testing.T, dir, service string, sql bool) *mcp.ClientSession {
	t.Helper()

	opts := engine.Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir}
	if service != "" {
		opts.Service = service
		opts.AllowedDir = filepath.Join(dir, "services", manifest.ServiceDir(service))
	}
	e, err := engine.Open(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	r := &tools.Runner{Eng: e, Now: func() time.Time { return toolstest.Now }, Service: service}
	srv := mcpserver.New(r, mcpserver.Options{SQL: sql, Version: "test"})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	st, ct := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	return cs
}

func TestListTools(t *testing.T) {
	dir := toolstest.Fixture(t)
	for sql, want := range map[bool]int{true: 6, false: 5} {
		cs := connect(t, dir, "", sql)
		res, err := cs.ListTools(t.Context(), nil)
		require.NoError(t, err)
		require.Len(t, res.Tools, want)
		names := []string{}
		for _, tool := range res.Tools {
			names = append(names, tool.Name)
			require.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
			require.NotNil(t, tool.Annotations.OpenWorldHint, tool.Name)
			require.False(t, *tool.Annotations.OpenWorldHint, tool.Name)
			require.NotEmpty(t, tool.Description)
		}
		require.Contains(t, names, "get_trace")
		require.Equal(t, sql, slices.Contains(names, "sql"))
	}
}

func call(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()

	res, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)

	return res
}

func text(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()

	require.Len(t, res.Content, 1)
	tc, ok := res.Content[0].(*mcp.TextContent)
	require.True(t, ok)

	return tc.Text
}

func TestCallEachTool(t *testing.T) {
	cs := connect(t, toolstest.Fixture(t), "", true)
	tests := []struct {
		name    string
		args    map[string]any
		summary string
		field   string
	}{
		{"list_services", map[string]any{}, "2 rows", "rows"},
		{"describe_schema", map[string]any{}, "", "logs"},
		{"search_logs", map[string]any{"min_severity": "error"}, "1 row", "rows"},
		{"find_spans", map[string]any{"status": "error", "limit": 1}, "1 row (truncated)", "rows"},
		{"get_trace", map[string]any{"trace_id": toolstest.TraceID}, "4 spans, 1 log", "spans"},
		{"sql", map[string]any{"query": "SELECT count(*) AS n FROM logs"}, "1 row", "rows"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := call(t, cs, tc.name, tc.args)
			require.False(t, res.IsError, text(t, res))
			require.NotNil(t, res.StructuredContent)
			m, ok := res.StructuredContent.(map[string]any)
			require.True(t, ok)
			require.Contains(t, m, tc.field)
			require.Contains(t, m["note"], "untrusted")
			if tc.summary != "" {
				require.Equal(t, tc.summary, text(t, res))
			}
		})
	}
}

func TestInputErrorIsToolError(t *testing.T) {
	cs := connect(t, toolstest.Fixture(t), "", true)

	res := call(t, cs, "search_logs", map[string]any{"regex": "("})
	require.True(t, res.IsError)
	require.Contains(t, text(t, res), "regex does not compile")

	res = call(t, cs, "get_trace", map[string]any{"trace_id": "nope"})
	require.True(t, res.IsError)
	require.Contains(t, text(t, res), "32 hex")

	res = call(t, cs, "sql", map[string]any{"query": "COPY logs TO 'x.csv'"})
	require.True(t, res.IsError)
}

func TestScopedSQLCannotReadOtherService(t *testing.T) {
	dir := toolstest.Fixture(t)
	var other []string
	err := filepath.WalkDir(filepath.Join(dir, "services", manifest.ServiceDir("web")), func(path string, _ fs.DirEntry, err error) error {
		if err == nil && filepath.Ext(path) == ".parquet" {
			other = append(other, path)
		}

		return err
	})
	require.NoError(t, err)
	require.NotEmpty(t, other)

	q := map[string]any{"query": "SELECT * FROM read_parquet('" + other[0] + "')"}
	// The same read works unscoped, so the failure below comes from scoping.
	res := call(t, connect(t, dir, "", true), "sql", q)
	require.False(t, res.IsError, text(t, res))

	cs := connect(t, dir, "agent", true)
	res = call(t, cs, "sql", q)
	require.True(t, res.IsError)

	res = call(t, cs, "sql", map[string]any{"query": "SELECT DISTINCT service_name FROM logs"})
	require.False(t, res.IsError, text(t, res))
	rows := res.StructuredContent.(map[string]any)["rows"].([]any)
	require.Len(t, rows, 1)
}

func TestSQLDescriptionHasNoWindow(t *testing.T) {
	cs := connect(t, toolstest.Fixture(t), "", true)
	res, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range res.Tools {
		if tool.Name == "sql" {
			require.NotContains(t, tool.Description, "since")
			require.Contains(t, tool.Description, "No time window")
		}
		if tool.Name == "search_logs" {
			b, err := json.Marshal(tool.InputSchema)
			require.NoError(t, err)
			require.Contains(t, string(b), "Defaults to 20")
			require.NotContains(t, string(b), "Defaults to 100")
		}
	}
}

func TestSQLDescriptionListsEveryColumn(t *testing.T) {
	cs := connect(t, toolstest.Fixture(t), "", true)
	var desc string
	tl, err := cs.ListTools(t.Context(), nil)
	require.NoError(t, err)
	for _, tool := range tl.Tools {
		if tool.Name == "sql" {
			desc = tool.Description
		}
	}
	require.NotEmpty(t, desc)

	for _, view := range []string{"logs", "spans"} {
		res := call(t, cs, "sql", map[string]any{"query": "SELECT column_name FROM (DESCRIBE " + view + ")"})
		require.False(t, res.IsError, text(t, res))
		rows := res.StructuredContent.(map[string]any)["rows"].([]any)
		require.NotEmpty(t, rows)
		for _, row := range rows {
			name := row.(map[string]any)["column_name"].(string)
			// Names are listed as "name," or "name)" or "name MAP"; a bare substring would match prefixes such as time in start_time.
			re := regexp.MustCompile(`[(, ]` + regexp.QuoteMeta(name) + `[,) ]`)
			require.Regexp(t, re, desc, "%s column %s", view, name)
		}
	}
}
