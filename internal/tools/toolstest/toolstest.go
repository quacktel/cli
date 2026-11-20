// Package toolstest builds a synthetic data dir for tests of the typed tools and the MCP server.
package toolstest

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/store"
)

const (
	TraceID      = "0102030405060708090a0b0c0d0e0f10"
	OtherTraceID = "ffffffffffffffffffffffffffffffff"
	LongBodyLen  = 2500
	LongAttrLen  = 5000
)

var Now = time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

var (
	traceBytes  = pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	otherTrace  = pcommon.TraceID{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rootID      = pcommon.SpanID{1, 1, 1, 1, 1, 1, 1, 1}
	childID     = pcommon.SpanID{2, 2, 2, 2, 2, 2, 2, 2}
	grandID     = pcommon.SpanID{3, 3, 3, 3, 3, 3, 3, 3}
	orphanID    = pcommon.SpanID{4, 4, 4, 4, 4, 4, 4, 4}
	missingID   = pcommon.SpanID{9, 9, 9, 9, 9, 9, 9, 9}
	boomID      = pcommon.SpanID{5, 5, 5, 5, 5, 5, 5, 5}
	oldSpanID   = pcommon.SpanID{6, 6, 6, 6, 6, 6, 6, 6}
	webSpanID   = pcommon.SpanID{7, 7, 7, 7, 7, 7, 7, 7}
	ts          = func(d time.Duration) pcommon.Timestamp { return pcommon.NewTimestampFromTime(Now.Add(d)) }
	noParentIDs = pcommon.SpanID{}
)

type logSpec struct {
	at       time.Duration
	sev      plog.SeverityNumber
	sevText  string
	body     string
	event    string
	session  string
	attrs    map[string]string
	trace    pcommon.TraceID
	span     pcommon.SpanID
	hasTrace bool
}

func addLogs(ld plog.Logs, service string, specs []logSpec) {
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	sl := rl.ScopeLogs().AppendEmpty()
	for _, s := range specs {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(ts(s.at))
		lr.SetSeverityNumber(s.sev)
		lr.SetSeverityText(s.sevText)
		lr.Body().SetStr(s.body)
		if s.event != "" {
			lr.SetEventName(s.event)
		}
		if s.session != "" {
			lr.Attributes().PutStr("session.id", s.session)
		}
		for k, v := range s.attrs {
			lr.Attributes().PutStr(k, v)
		}
		if s.hasTrace {
			lr.SetTraceID(s.trace)
			lr.SetSpanID(s.span)
		}
	}
}

type spanSpec struct {
	name   string
	id     pcommon.SpanID
	parent pcommon.SpanID
	trace  pcommon.TraceID
	kind   ptrace.SpanKind
	status ptrace.StatusCode
	start  time.Duration
	dur    time.Duration
	attrs  map[string]string
	events []string
}

func addSpans(td ptrace.Traces, service string, specs []spanSpec) {
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	ss := rs.ScopeSpans().AppendEmpty()
	for _, s := range specs {
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(s.trace)
		sp.SetSpanID(s.id)
		sp.SetParentSpanID(s.parent)
		sp.SetName(s.name)
		sp.SetKind(s.kind)
		sp.Status().SetCode(s.status)
		sp.SetStartTimestamp(ts(s.start))
		sp.SetEndTimestamp(ts(s.start + s.dur))
		for k, v := range s.attrs {
			sp.Attributes().PutStr(k, v)
		}
		for _, e := range s.events {
			ev := sp.Events().AppendEmpty()
			ev.SetName(e)
			ev.SetTimestamp(ts(s.start))
		}
	}
}

// Fixture writes two services ("agent" and "web") with logs and spans, including records older than 24h, and returns the data dir.
func Fixture(t *testing.T) string {
	t.Helper()

	return fixture(t, 0)
}

// FixtureBulkLogs is Fixture plus n extra recent "bulk" logs in the agent service.
func FixtureBulkLogs(t *testing.T, n int) string {
	t.Helper()

	return fixture(t, n)
}

func fixture(t *testing.T, bulk int) string {
	t.Helper()

	t.Setenv("HOME", t.TempDir())
	dir := filepath.Join(t.TempDir(), "d")
	s, err := store.Open(t.Context(), store.Options{DataDir: dir, Now: func() time.Time { return Now }})
	require.NoError(t, err)

	ld := plog.NewLogs()
	addLogs(ld, "agent", []logSpec{
		{at: -time.Hour, sev: plog.SeverityNumberInfo, sevText: "INFO", body: "ran tool bash", event: "tool.call", session: "s1",
			attrs: map[string]string{"tool": "bash", "odd'key": "v"}},
		{at: -30 * time.Minute, sev: plog.SeverityNumberError, sevText: "ERROR", body: "failure: code=E42 boom", attrs: map[string]string{"tool": "read"}},
		{at: -20 * time.Minute, sev: plog.SeverityNumberDebug, sevText: "DEBUG", body: "debug noise",
			attrs: map[string]string{"blob": strings.Repeat("y", LongAttrLen)}},
		{at: -10 * time.Minute, sev: plog.SeverityNumberInfo, sevText: "INFO", body: strings.Repeat("x", LongBodyLen)},
		{at: -5 * time.Minute, sev: plog.SeverityNumberWarn, sevText: "WARN", body: "retrying", trace: traceBytes, span: childID, hasTrace: true},
		{at: -30 * time.Hour, sev: plog.SeverityNumberInfo, sevText: "INFO", body: "ancient"},
	})
	if bulk > 0 {
		specs := make([]logSpec, bulk)
		for i := range specs {
			specs[i] = logSpec{at: -2 * time.Minute, sev: plog.SeverityNumberInfo, sevText: "INFO", body: "bulk"}
		}
		addLogs(ld, "agent", specs)
	}
	addLogs(ld, "web", []logSpec{
		{at: -2 * time.Hour, sev: plog.SeverityNumberInfo, sevText: "INFO", body: "web request"},
	})
	_, _, err = s.AppendLogs(t.Context(), ld)
	require.NoError(t, err)

	td := ptrace.NewTraces()
	base := -15 * time.Minute
	addSpans(td, "agent", []spanSpec{
		{name: "root", id: rootID, parent: noParentIDs, trace: traceBytes, kind: ptrace.SpanKindServer, status: ptrace.StatusCodeOk,
			start: base, dur: 3 * time.Second, attrs: map[string]string{"http.route": "/v1/x", "session.id": "s1"}},
		{name: "chat", id: childID, parent: rootID, trace: traceBytes, kind: ptrace.SpanKindClient,
			start: base + time.Second, dur: time.Second, attrs: map[string]string{"gen_ai.request.model": "model-x"}, events: []string{"retry"}},
		{name: "tool", id: grandID, parent: childID, trace: traceBytes, kind: ptrace.SpanKindInternal, status: ptrace.StatusCodeError,
			start: base + 1200*time.Millisecond, dur: 100 * time.Millisecond},
		{name: "orphan", id: orphanID, parent: missingID, trace: traceBytes, kind: ptrace.SpanKindInternal,
			start: base + 2*time.Second, dur: time.Millisecond},
		{name: "boom", id: boomID, parent: noParentIDs, trace: otherTrace, kind: ptrace.SpanKindInternal, status: ptrace.StatusCodeError,
			start: -time.Hour, dur: 5 * time.Second, attrs: map[string]string{"tool": "bash"}},
		{name: "ancient", id: oldSpanID, parent: noParentIDs, trace: otherTrace, kind: ptrace.SpanKindInternal,
			start: -30 * time.Hour, dur: time.Second},
	})
	addSpans(td, "web", []spanSpec{
		{name: "GET /", id: webSpanID, parent: noParentIDs, trace: otherTrace, kind: ptrace.SpanKindServer,
			start: -2 * time.Hour, dur: 20 * time.Millisecond},
	})
	_, _, err = s.AppendTraces(t.Context(), td)
	require.NoError(t, err)

	require.NoError(t, s.Flush(t.Context()))
	require.NoError(t, s.Close())

	return dir
}
