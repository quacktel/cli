package store

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/quacktel/cli/internal/engine"
	"github.com/quacktel/cli/internal/manifest"
)

var (
	testTraceID = pcommon.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rootSpanID  = pcommon.SpanID{1, 1, 1, 1, 1, 1, 1, 1}
	clientID    = pcommon.SpanID{2, 2, 2, 2, 2, 2, 2, 2}
	internalID  = pcommon.SpanID{3, 3, 3, 3, 3, 3, 3, 3}
)

func tsAt(d time.Duration) pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(fixedNow.Add(d))
}

func spansFor(services ...string) ptrace.Traces {
	td := ptrace.NewTraces()
	for _, name := range services {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", name)
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetTraceID(testTraceID)
		sp.SetSpanID(rootSpanID)
		sp.SetName("op")
		sp.SetStartTimestamp(tsAt(0))
		sp.SetEndTimestamp(tsAt(time.Second))
	}

	return td
}

func traceFixture() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "agent")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("scope")
	ss.Scope().SetVersion("1.0")

	root := ss.Spans().AppendEmpty()
	root.SetTraceID(testTraceID)
	root.SetSpanID(rootSpanID)
	root.SetName("root")
	root.SetKind(ptrace.SpanKindServer)
	root.SetStartTimestamp(tsAt(0))
	root.SetEndTimestamp(tsAt(3 * time.Second))
	root.Status().SetCode(ptrace.StatusCodeOk)
	root.Attributes().PutStr("http.request.method", "GET")
	root.Attributes().PutStr("http.response.status_code", "200")
	root.Attributes().PutStr("http.route", "/v1/x")
	root.Attributes().PutStr("session.id", "sess-1")

	client := ss.Spans().AppendEmpty()
	client.SetTraceID(testTraceID)
	client.SetSpanID(clientID)
	client.SetParentSpanID(rootSpanID)
	client.SetName("chat")
	client.SetKind(ptrace.SpanKindClient)
	client.SetStartTimestamp(tsAt(time.Second))
	client.SetEndTimestamp(tsAt(2 * time.Second))
	client.Attributes().PutStr("gen_ai.request.model", "model-x")
	client.Attributes().PutInt("gen_ai.usage.input_tokens", 11)
	client.Attributes().PutInt("gen_ai.usage.output_tokens", 22)
	ev := client.Events().AppendEmpty()
	ev.SetName("retry")
	ev.SetTimestamp(tsAt(1500 * time.Millisecond))
	ev.Attributes().PutStr("k", "v")
	ln := client.Links().AppendEmpty()
	ln.SetTraceID(pcommon.TraceID{0xaa, 0xbb, 0xcc, 0xdd, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	ln.SetSpanID(pcommon.SpanID{9, 9, 9, 9, 9, 9, 9, 9})

	internal := ss.Spans().AppendEmpty()
	internal.SetTraceID(testTraceID)
	internal.SetSpanID(internalID)
	internal.SetParentSpanID(rootSpanID)
	internal.SetName("boom")
	internal.SetKind(ptrace.SpanKindInternal)
	internal.SetStartTimestamp(tsAt(2 * time.Second))
	internal.SetEndTimestamp(tsAt(2*time.Second + 5))
	internal.Status().SetCode(ptrace.StatusCodeError)
	internal.Status().SetMessage("failed")
	ex := internal.Events().AppendEmpty()
	ex.SetName("exception")
	ex.Attributes().PutStr("exception.type", "E")

	return td
}

func spansEngine(t *testing.T, dataDir string) *engine.Engine {
	t.Helper()

	e, err := engine.Open(t.Context(), engine.Options{AllowedDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })

	return e
}

func TestSpansRoundTrip(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	rejected, msg, err := s.AppendTraces(t.Context(), traceFixture())
	require.NoError(t, err)
	require.Zero(t, rejected)
	require.Empty(t, msg)
	require.EqualValues(t, 3, s.Stats().SpansAccepted)
	require.NoError(t, s.Flush(t.Context()))

	e := spansEngine(t, dataDir)
	q := "SELECT name, kind, status_code, status_message, duration_ns, span_id, parent_span_id, start_time, " +
		"http_response_status_code, http_request_method, http_route, session_id, " +
		"gen_ai_request_model, gen_ai_usage_input_tokens, gen_ai_usage_output_tokens, " +
		"events[1].name AS ev_name, events[1].attrs['k'] AS ev_k, len(events) AS n_events, " +
		"links[1].trace_id AS link_trace, len(links) AS n_links, typeof(duration_ns) AS dur_type, attrs['http.route'] AS route_attr " +
		"FROM read_parquet('" + dataDir + "/services/*/spans/**/*.parquet') ORDER BY start_time"
	res, err := e.Query(t.Context(), q, 10)
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)
	rows := make([]map[string]any, len(res.Rows))
	for i, r := range res.Rows {
		rows[i] = map[string]any{}
		for j, c := range res.Columns {
			rows[i][c] = r[j]
		}
	}
	root, client, internal := rows[0], rows[1], rows[2]

	require.Equal(t, "server", root["kind"])
	require.Equal(t, "ok", root["status_code"])
	require.EqualValues(t, 3*time.Second, root["duration_ns"])
	require.Equal(t, "BIGINT", root["dur_type"])
	require.Nil(t, root["parent_span_id"])
	require.Equal(t, "0101010101010101", root["span_id"])
	require.EqualValues(t, 200, root["http_response_status_code"])
	require.Equal(t, "GET", root["http_request_method"])
	require.Equal(t, "/v1/x", root["http_route"])
	require.Equal(t, "/v1/x", root["route_attr"])
	require.Equal(t, "sess-1", root["session_id"])
	require.IsType(t, time.Time{}, root["start_time"])
	require.Equal(t, fixedNow.UnixNano(), root["start_time"].(time.Time).UnixNano())

	require.Equal(t, "client", client["kind"])
	require.Equal(t, "unset", client["status_code"])
	require.Equal(t, "0101010101010101", client["parent_span_id"])
	require.EqualValues(t, time.Second, client["duration_ns"])
	require.Equal(t, "model-x", client["gen_ai_request_model"])
	require.EqualValues(t, 11, client["gen_ai_usage_input_tokens"])
	require.EqualValues(t, 22, client["gen_ai_usage_output_tokens"])
	require.Equal(t, "retry", client["ev_name"])
	require.Equal(t, "v", client["ev_k"])
	require.EqualValues(t, 1, client["n_events"])
	require.Equal(t, "aabbccdd0102030405060708090a0b0c", client["link_trace"])
	require.EqualValues(t, 1, client["n_links"])

	require.Equal(t, "internal", internal["kind"])
	require.Equal(t, "error", internal["status_code"])
	require.Equal(t, "failed", internal["status_message"])
	require.Equal(t, "0101010101010101", internal["parent_span_id"])
	require.Equal(t, "exception", internal["ev_name"])
	require.EqualValues(t, 5, internal["duration_ns"])
	require.Nil(t, internal["link_trace"])
}

func TestMixedBatchWritesPerSignalFiles(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir})

	_, _, err := s.AppendLogs(t.Context(), logsFor("mixed"))
	require.NoError(t, err)
	_, _, err = s.AppendTraces(t.Context(), spansFor("mixed"))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))

	svcDir := filepath.Join(dataDir, "services", ServiceDir("mixed"))
	require.Len(t, parquetFiles(t, filepath.Join(svcDir, "logs")), 1)
	spanFiles := parquetFiles(t, filepath.Join(svcDir, "spans"))
	require.Len(t, spanFiles, 1)
	require.Contains(t, spanFiles[0], filepath.Join("spans", "date=2026-11-20", "hour=13", "tail-"))

	m := readManifest(t, dataDir, "mixed")
	require.Len(t, m.Files, 2)
	bySignal := map[string]manifest.File{}
	for _, f := range m.Files {
		bySignal[f.Signal] = f
		require.True(t, strings.HasPrefix(f.Path, f.Signal+"/"), f.Path)
		require.EqualValues(t, 1, f.Rows)
	}
	require.Contains(t, bySignal, "logs")
	require.Contains(t, bySignal, "spans")
	require.Equal(t, 1, s.Stats().Services)
	require.EqualValues(t, 2, s.Stats().Flushes)
}

func TestReconcileSpans(t *testing.T) {
	dataDir := newDataDir(t)
	t.Setenv("HOME", t.TempDir())
	s, err := Open(t.Context(), Options{DataDir: dataDir, Now: func() time.Time { return fixedNow }})
	require.NoError(t, err)
	_, _, err = s.AppendTraces(t.Context(), spansFor("svc"))
	require.NoError(t, err)
	require.NoError(t, s.Flush(t.Context()))
	require.NoError(t, s.Close())

	orig := parquetFiles(t, dataDir)[0]
	src, err := os.Open(orig)
	require.NoError(t, err)
	defer src.Close()
	extra := filepath.Join(filepath.Dir(orig), "tail-01EXTRAEXTRAEXTRAEXTRAEXTR.parquet")
	dst, err := os.OpenFile(extra, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = io.Copy(dst, src)
	require.NoError(t, err)
	require.NoError(t, dst.Close())

	s2, err := Open(t.Context(), Options{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, s2.Close())

	m := readManifest(t, dataDir, "svc")
	require.Len(t, m.Files, 2)
	for _, f := range m.Files {
		require.Equal(t, "spans", f.Signal)
		require.EqualValues(t, 1, f.Rows)
	}
}

func TestSpansBufferCap(t *testing.T) {
	dataDir := newDataDir(t)
	s := openStore(t, Options{DataDir: dataDir, BufferBytes: 10})

	_, _, err := s.AppendTraces(t.Context(), spansFor("svc"))
	require.ErrorIs(t, err, ErrFull)
	require.Zero(t, s.Stats().SpansAccepted)
	require.Zero(t, s.Stats().Services)
	require.NoError(t, s.Flush(t.Context()))
	require.Empty(t, parquetFiles(t, dataDir))
}

func TestBufferCapCountsAllSignals(t *testing.T) {
	td := spansFor("svc")
	size := int64((&ptrace.ProtoMarshaler{}).TracesSize(td))
	s := openStore(t, Options{BufferBytes: 1 << 20})

	_, _, err := s.AppendLogs(t.Context(), logsFor("svc"))
	require.NoError(t, err)
	// The spans batch alone fits under this cap; only the buffered logs push it over.
	s.opts.BufferBytes = s.buffered() + size - 1
	require.Greater(t, s.opts.BufferBytes, size)
	_, _, err = s.AppendTraces(t.Context(), td)
	require.ErrorIs(t, err, ErrFull)
	require.Zero(t, s.Stats().SpansAccepted)
}

func TestServiceCapSharedAcrossSignals(t *testing.T) {
	s := openStore(t, Options{MaxServices: 1})

	rejected, _, err := s.AppendLogs(t.Context(), logsFor("a"))
	require.NoError(t, err)
	require.Zero(t, rejected)
	rejected, _, err = s.AppendTraces(t.Context(), spansFor("a"))
	require.NoError(t, err)
	require.Zero(t, rejected)

	rejected, msg, err := s.AppendTraces(t.Context(), spansFor("b"))
	require.NoError(t, err)
	require.EqualValues(t, 1, rejected)
	require.Equal(t, "service limit reached", msg)
	require.EqualValues(t, 1, s.Stats().SpansAccepted)
	require.EqualValues(t, 1, s.Stats().SpansRejected)
	require.Equal(t, 1, s.Stats().Services)
}
