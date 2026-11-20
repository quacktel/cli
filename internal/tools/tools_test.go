package tools_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
	"github.com/quacktel/cli/internal/tools"
	"github.com/quacktel/cli/internal/tools/toolstest"
)

func newRunner(t *testing.T, service string) *tools.Runner {
	t.Helper()

	return newRunnerDir(t, toolstest.Fixture(t), service)
}

func newRunnerDir(t *testing.T, dir, service string) *tools.Runner {
	t.Helper()

	opts := engine.Options{AllowedDir: filepath.Join(dir, "services"), DataDir: dir}
	if service != "" {
		opts.Service = service
		opts.AllowedDir = filepath.Join(dir, "services", manifest.ServiceDir(service))
	}
	e, err := engine.Open(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	return &tools.Runner{Eng: e, Now: func() time.Time { return toolstest.Now }, Service: service}
}

func col(t *testing.T, rows tools.Rows, name string) []any {
	t.Helper()

	out := []any{}
	for _, r := range rows.Rows {
		v, ok := r[name]
		require.True(t, ok, "missing column %s", name)
		out = append(out, v)
	}

	return out
}

func TestListServices(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.ListServices(t.Context(), tools.ListServicesInput{})
	require.NoError(t, err)
	require.Equal(t, []any{"agent", "web"}, col(t, rows, "service"))
	require.Equal(t, []any{int64(5), int64(1)}, col(t, rows, "logs"))
	require.Equal(t, []any{int64(5), int64(1)}, col(t, rows, "spans"))
	require.Contains(t, rows.Note, "untrusted")
	require.IsType(t, "", rows.Rows[0]["first_seen"])

	_, err = json.Marshal(rows)
	require.NoError(t, err)

	rows, err = r.ListServices(t.Context(), tools.ListServicesInput{Since: "48h"})
	require.NoError(t, err)
	require.Equal(t, []any{int64(6), int64(1)}, col(t, rows, "logs"))
	require.Equal(t, []any{int64(6), int64(1)}, col(t, rows, "spans"))
}

func TestDescribeSchema(t *testing.T) {
	r := newRunner(t, "")
	out, err := r.DescribeSchema(t.Context(), tools.DescribeSchemaInput{})
	require.NoError(t, err)
	require.Contains(t, out.Note, "untrusted")

	names := []string{}
	for _, c := range out.Logs.Columns {
		names = append(names, c.Name)
	}
	require.Contains(t, names, "body")
	require.Equal(t, "event_name", names[5])
	names = names[:0]
	for _, c := range out.Spans.Columns {
		names = append(names, c.Name)
	}
	require.Contains(t, names, "duration_ns")

	keys := map[string]tools.AttributeKey{}
	for _, k := range out.Logs.TopAttributeKeys {
		keys[k.Key] = k
	}
	require.Equal(t, int64(2), keys["tool"].Count)
	require.Contains(t, []string{"bash", "read"}, keys["tool"].Example)
	require.Contains(t, keys, "odd'key")
	spanKeys := []string{}
	for _, k := range out.Spans.TopAttributeKeys {
		spanKeys = append(spanKeys, k.Key)
	}
	require.Contains(t, spanKeys, "tool")

	out, err = r.DescribeSchema(t.Context(), tools.DescribeSchemaInput{Service: "web"})
	require.NoError(t, err)
	require.Empty(t, out.Logs.TopAttributeKeys)

	_, err = json.Marshal(out)
	require.NoError(t, err)
}

func TestSearchLogsFilters(t *testing.T) {
	r := newRunner(t, "")
	tests := []struct {
		name   string
		in     tools.SearchLogsInput
		bodies []string
	}{
		{"default window excludes old", tools.SearchLogsInput{Service: "agent", MinSeverity: "info", Text: "ran"}, []string{"ran tool bash"}},
		{"min severity text", tools.SearchLogsInput{MinSeverity: "ERROR"}, []string{"failure: code=E42 boom"}},
		{"min severity number", tools.SearchLogsInput{MinSeverity: "13"}, []string{"retrying", "failure: code=E42 boom"}},
		{"text case-insensitive", tools.SearchLogsInput{Text: "NOISE"}, []string{"debug noise"}},
		{"regex", tools.SearchLogsInput{Regex: `code=E\d+`}, []string{"failure: code=E42 boom"}},
		{"event name", tools.SearchLogsInput{EventName: "tool.call"}, []string{"ran tool bash"}},
		{"session", tools.SearchLogsInput{SessionID: "s1"}, []string{"ran tool bash"}},
		{"trace", tools.SearchLogsInput{TraceID: strings.ToUpper(toolstest.TraceID)}, []string{"retrying"}},
		{"attrs key and value", tools.SearchLogsInput{Attrs: map[string]string{"tool": "read"}}, []string{"failure: code=E42 boom"}},
		{"attrs mismatch", tools.SearchLogsInput{Attrs: map[string]string{"tool": "nope"}}, []string{}},
		{"attrs key with quote", tools.SearchLogsInput{Attrs: map[string]string{"odd'key": "v"}}, []string{"ran tool bash"}},
		{"service", tools.SearchLogsInput{Service: "web"}, []string{"web request"}},
		{"since 48h includes old", tools.SearchLogsInput{Since: "48h", Text: "ancient"}, []string{"ancient"}},
		{"until excludes recent", tools.SearchLogsInput{Since: "3h", Until: "1h30m"}, []string{"web request"}},
		{"rfc3339 since", tools.SearchLogsInput{Since: toolstest.Now.Add(-25 * time.Minute).Format(time.RFC3339), MinSeverity: "debug"}, []string{"retrying", "x", "debug noise"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := r.SearchLogs(t.Context(), tc.in)
			require.NoError(t, err)
			got := []string{}
			for _, v := range col(t, rows, "body") {
				s := v.(string)
				if strings.HasPrefix(s, "xxx") {
					s = "x"
				} else if len(s) > 100 {
					s = "long"
				}
				got = append(got, s)
			}
			require.Equal(t, tc.bodies, got)
		})
	}
}

func TestSearchLogsDefaultWindow(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SearchLogs(t.Context(), tools.SearchLogsInput{})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 6)
	require.NotContains(t, col(t, rows, "body"), "ancient")
	// Time DESC: web request is oldest of the in-window records.
	require.Equal(t, "web request", rows.Rows[5]["body"])

	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{Since: "48h"})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 7)
}

func TestSearchLogsTruncation(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SearchLogs(t.Context(), tools.SearchLogsInput{MinSeverity: "info", Regex: "^x+$"})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 1)
	body := rows.Rows[0]["body"].(string)
	require.True(t, rows.Truncated)
	require.True(t, strings.HasSuffix(body, "…[truncated 500 chars]"), body[len(body)-30:])
	require.Equal(t, 2000+utf8.RuneCountInString("…[truncated 500 chars]"), utf8.RuneCountInString(body))
}

func TestSearchLogsLimit(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SearchLogs(t.Context(), tools.SearchLogsInput{Limit: 2})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 2)
	require.True(t, rows.Truncated)
	require.NotContains(t, rows.Note, "size cap")

	for _, limit := range []int{0, -5, 10_000} {
		rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{Limit: limit})
		require.NoError(t, err)
		require.Len(t, rows.Rows, 6)
	}
}

func TestSearchLogsDefaultLimit(t *testing.T) {
	r := newRunnerDir(t, toolstest.FixtureBulkLogs(t, 40), "")
	rows, err := r.SearchLogs(t.Context(), tools.SearchLogsInput{})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 20)
	require.True(t, rows.Truncated)
	require.NotContains(t, rows.Note, "size cap")

	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{Limit: 30})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 30)
}

func TestResultBudget(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SQL(t.Context(), tools.SQLInput{Query: "SELECT repeat('x', 1900) AS s FROM range(500)", Limit: 500})
	require.NoError(t, err)
	require.True(t, rows.Truncated)
	require.NotEmpty(t, rows.Rows)
	require.Less(t, len(rows.Rows), 500)
	require.Contains(t, rows.Note, "untrusted")
	require.Contains(t, rows.Note, "size cap")
	b, err := json.Marshal(rows.Rows)
	require.NoError(t, err)
	require.LessOrEqual(t, len(b), 40000)
}

func TestDescribeSchemaEventName(t *testing.T) {
	r := newRunner(t, "")
	out, err := r.DescribeSchema(t.Context(), tools.DescribeSchemaInput{EventName: "tool.call"})
	require.NoError(t, err)
	keys := map[string]tools.AttributeKey{}
	for _, k := range out.Logs.TopAttributeKeys {
		keys[k.Key] = k
	}
	require.Equal(t, int64(1), keys["tool"].Count)
	require.Contains(t, keys, "odd'key")
	spanKeys := []string{}
	for _, k := range out.Spans.TopAttributeKeys {
		spanKeys = append(spanKeys, k.Key)
	}
	require.Contains(t, spanKeys, "tool")

	out, err = r.DescribeSchema(t.Context(), tools.DescribeSchemaInput{EventName: "no.such.event"})
	require.NoError(t, err)
	require.Empty(t, out.Logs.TopAttributeKeys)
	require.NotEmpty(t, out.Spans.TopAttributeKeys)
}

func TestDescribeSchemaBudget(t *testing.T) {
	r := newRunner(t, "")
	out, err := r.DescribeSchema(t.Context(), tools.DescribeSchemaInput{})
	require.NoError(t, err)
	var blob *tools.AttributeKey
	for _, k := range out.Logs.TopAttributeKeys {
		if k.Key == "blob" {
			blob = &k
		}
	}
	require.NotNil(t, blob)
	require.True(t, out.Truncated)
	require.Equal(t, 200+utf8.RuneCountInString("…[truncated 4800 chars]"), utf8.RuneCountInString(blob.Example))
	b, err := json.Marshal(out)
	require.NoError(t, err)
	require.Less(t, len(b), 10000)
}

func TestLimitDefaultAndClamp(t *testing.T) {
	r := newRunner(t, "")
	for limit, want := range map[int]int{0: 100, 10_000: 500} {
		rows, err := r.SQL(t.Context(), tools.SQLInput{Query: "SELECT * FROM range(1000)", Limit: limit})
		require.NoError(t, err)
		require.Len(t, rows.Rows, want)
		require.True(t, rows.Truncated)
	}
}

func TestFindSpansFilters(t *testing.T) {
	r := newRunner(t, "")
	tests := []struct {
		name  string
		in    tools.FindSpansInput
		names []string
	}{
		{"default window slowest first", tools.FindSpansInput{}, []string{"boom", "root", "chat", "tool", "GET /", "orphan"}},
		{"name ilike", tools.FindSpansInput{Name: "CHA"}, []string{"chat"}},
		{"kind", tools.FindSpansInput{Kind: "server"}, []string{"root", "GET /"}},
		{"status error", tools.FindSpansInput{Status: "error"}, []string{"boom", "tool"}},
		{"status ok", tools.FindSpansInput{Status: "ok"}, []string{"root"}},
		{"min duration", tools.FindSpansInput{MinDurationMS: 2000}, []string{"boom", "root"}},
		{"route", tools.FindSpansInput{HTTPRoute: "/v1/x"}, []string{"root"}},
		{"attrs", tools.FindSpansInput{Attrs: map[string]string{"tool": "bash"}}, []string{"boom"}},
		{"service", tools.FindSpansInput{Service: "web"}, []string{"GET /"}},
		{"48h includes old", tools.FindSpansInput{Since: "48h", Name: "ancient"}, []string{"ancient"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := r.FindSpans(t.Context(), tc.in)
			require.NoError(t, err)
			require.Equal(t, anys(tc.names), col(t, rows, "name"))
		})
	}

	rows, err := r.FindSpans(t.Context(), tools.FindSpansInput{Name: "chat"})
	require.NoError(t, err)
	require.Equal(t, "model-x", rows.Rows[0]["gen_ai_request_model"])
	require.Equal(t, int64(time.Second), rows.Rows[0]["duration_ns"])
	_, err = json.Marshal(rows)
	require.NoError(t, err)
}

func anys(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}

	return out
}

func TestGetTrace(t *testing.T) {
	r := newRunner(t, "")
	out, err := r.GetTrace(t.Context(), tools.GetTraceInput{TraceID: strings.ToUpper(toolstest.TraceID)})
	require.NoError(t, err)
	require.Contains(t, out.Note, "untrusted")

	type node struct {
		name  string
		depth int
	}
	got := []node{}
	for _, s := range out.Spans {
		got = append(got, node{s["name"].(string), s["depth"].(int)})
	}
	require.Equal(t, []node{{"root", 0}, {"chat", 1}, {"tool", 2}, {"orphan", 0}}, got)
	require.Equal(t, []any{"retry"}, out.Spans[1]["event_names"])
	require.Equal(t, []any{}, out.Spans[0]["event_names"])

	require.Len(t, out.Logs, 1)
	require.Equal(t, "retrying", out.Logs[0]["body"])

	_, err = json.Marshal(out)
	require.NoError(t, err)

	out, err = r.GetTrace(t.Context(), tools.GetTraceInput{TraceID: toolstest.TraceID, Service: "web"})
	require.NoError(t, err)
	require.Empty(t, out.Spans)
}

func TestGetTraceIgnoresWindow(t *testing.T) {
	r := newRunner(t, "")
	out, err := r.GetTrace(t.Context(), tools.GetTraceInput{TraceID: toolstest.OtherTraceID})
	require.NoError(t, err)
	require.Len(t, out.Spans, 3)
}

func TestInputErrors(t *testing.T) {
	r := newRunner(t, "")
	scoped := newRunner(t, "agent")
	ctx := t.Context()
	tests := map[string]func() error{
		"bad regex": func() error {
			_, err := r.SearchLogs(ctx, tools.SearchLogsInput{Regex: "("})
			return err
		},
		"bad duration": func() error {
			_, err := r.SearchLogs(ctx, tools.SearchLogsInput{Since: "soon"})
			return err
		},
		"negative duration": func() error {
			_, err := r.FindSpans(ctx, tools.FindSpansInput{Until: "-1h"})
			return err
		},
		"since after until": func() error {
			_, err := r.ListServices(ctx, tools.ListServicesInput{Since: "1h", Until: "2h"})
			return err
		},
		"bad severity": func() error {
			_, err := r.SearchLogs(ctx, tools.SearchLogsInput{MinSeverity: "loud"})
			return err
		},
		"severity range": func() error {
			_, err := r.SearchLogs(ctx, tools.SearchLogsInput{MinSeverity: "25"})
			return err
		},
		"bad log trace id": func() error {
			_, err := r.SearchLogs(ctx, tools.SearchLogsInput{TraceID: "abc"})
			return err
		},
		"bad trace id": func() error {
			_, err := r.GetTrace(ctx, tools.GetTraceInput{TraceID: strings.Repeat("z", 32)})
			return err
		},
		"missing trace id": func() error {
			_, err := r.GetTrace(ctx, tools.GetTraceInput{})
			return err
		},
		"bad kind": func() error {
			_, err := r.FindSpans(ctx, tools.FindSpansInput{Kind: "SERVER"})
			return err
		},
		"bad status": func() error {
			_, err := r.FindSpans(ctx, tools.FindSpansInput{Status: "bad"})
			return err
		},
		"scope mismatch logs": func() error {
			_, err := scoped.SearchLogs(ctx, tools.SearchLogsInput{Service: "web"})
			return err
		},
		"scope mismatch schema": func() error {
			_, err := scoped.DescribeSchema(ctx, tools.DescribeSchemaInput{Service: "web"})
			return err
		},
		"scope mismatch trace": func() error {
			_, err := scoped.GetTrace(ctx, tools.GetTraceInput{TraceID: toolstest.TraceID, Service: "web"})
			return err
		},
		"empty sql": func() error {
			_, err := r.SQL(ctx, tools.SQLInput{})
			return err
		},
	}
	for name, f := range tests {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, f(), tools.ErrInput)
		})
	}
}

func TestScopedRunner(t *testing.T) {
	r := newRunner(t, "agent")
	rows, err := r.ListServices(t.Context(), tools.ListServicesInput{})
	require.NoError(t, err)
	require.Equal(t, []any{"agent"}, col(t, rows, "service"))

	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{Service: "agent"})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 5)
	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 5)
}

func TestInjectionSafety(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SearchLogs(t.Context(), tools.SearchLogsInput{EventName: "x' OR 1=1 --"})
	require.NoError(t, err)
	require.Empty(t, rows.Rows)

	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{Attrs: map[string]string{"k'; DROP VIEW logs; --": "v'"}, Text: "'"})
	require.NoError(t, err)
	require.Empty(t, rows.Rows)

	spans, err := r.FindSpans(t.Context(), tools.FindSpansInput{Name: "' OR 1=1 --", HTTPRoute: "x' OR '1'='1"})
	require.NoError(t, err)
	require.Empty(t, spans.Rows)

	rows, err = r.SearchLogs(t.Context(), tools.SearchLogsInput{})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 6)
}

func TestSQL(t *testing.T) {
	r := newRunner(t, "")
	rows, err := r.SQL(t.Context(), tools.SQLInput{Query: "SELECT service_name, attrs, time FROM logs WHERE event_name = 'tool.call'"})
	require.NoError(t, err)
	require.Len(t, rows.Rows, 1)
	require.Equal(t, "bash", rows.Rows[0]["attrs"].(map[string]any)["tool"])
	require.IsType(t, "", rows.Rows[0]["time"])
	_, err = json.Marshal(rows)
	require.NoError(t, err)

	rows, err = r.SQL(t.Context(), tools.SQLInput{Query: "SELECT events FROM spans WHERE name = 'chat'"})
	require.NoError(t, err)
	_, err = json.Marshal(rows)
	require.NoError(t, err)

	for _, q := range []string{
		"COPY logs TO '/tmp/x.csv'",
		"ATTACH 'x.db'",
		"SELECT 1; SELECT 2",
		"SELECT * FROM read_parquet('/etc/passwd')",
		"SELEC nonsense",
	} {
		_, err := r.SQL(t.Context(), tools.SQLInput{Query: q})
		require.ErrorIs(t, err, tools.ErrInput, q)
	}
}
