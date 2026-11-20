package table

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func appendSpans(t *testing.T, spans ...ptrace.Span) arrow.RecordBatch {
	t.Helper()

	b := NewSpansBuilder(memory.NewGoAllocator())
	defer b.Release()
	recvd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, sp := range spans {
		b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), sp, recvd)
	}

	return b.NewRecord()
}

func TestSpansSchemaMetadata(t *testing.T) {
	v, ok := SpansSchema().Metadata().GetValue(SchemaVersionKey)
	require.True(t, ok)
	require.Equal(t, "1", v)
	sig, ok := SpansSchema().Metadata().GetValue(SignalKey)
	require.True(t, ok)
	require.Equal(t, "spans", sig)
	sig, ok = LogsSchema().Metadata().GetValue(SignalKey)
	require.True(t, ok)
	require.Equal(t, "logs", sig)
}

func TestSpanKindAndStatus(t *testing.T) {
	kinds := map[ptrace.SpanKind]string{
		ptrace.SpanKindUnspecified: "unspecified",
		ptrace.SpanKindInternal:    "internal",
		ptrace.SpanKindServer:      "server",
		ptrace.SpanKindClient:      "client",
		ptrace.SpanKindProducer:    "producer",
		ptrace.SpanKindConsumer:    "consumer",
	}
	for k, want := range kinds {
		require.Equal(t, want, kindName(k))
	}
	statuses := map[ptrace.StatusCode]string{
		ptrace.StatusCodeUnset: "unset",
		ptrace.StatusCodeOk:    "ok",
		ptrace.StatusCodeError: "error",
	}
	for c, want := range statuses {
		require.Equal(t, want, statusName(c))
	}
}

func TestSpanDuration(t *testing.T) {
	mk := func(start, end int64) ptrace.Span {
		sp := ptrace.NewSpan()
		sp.SetStartTimestamp(pcommon.Timestamp(start))
		sp.SetEndTimestamp(pcommon.Timestamp(end))

		return sp
	}
	rec := appendSpans(t, mk(100, 350), mk(500, 100), mk(0, 100), mk(100, 0))
	defer rec.Release()

	dur := rec.Column(spDuration).(*array.Int64)
	require.EqualValues(t, 250, dur.Value(0))
	for i := 1; i < 4; i++ {
		require.True(t, dur.IsNull(i), i)
	}
	start := rec.Column(spStart).(*array.Timestamp)
	require.True(t, start.IsNull(2))
	require.False(t, start.IsNull(3))
	require.True(t, rec.Column(spEnd).(*array.Timestamp).IsNull(3))
}

func TestSpanPromotedAttrs(t *testing.T) {
	sp := ptrace.NewSpan()
	a := sp.Attributes()
	a.PutStr("session.id", "s1")
	a.PutStr("http.response.status_code", "404")
	a.PutInt("gen_ai.usage.input_tokens", 12)
	a.PutStr("gen_ai.usage.output_tokens", "abc")
	a.PutStr("http.route", "/x")

	rec := appendSpans(t, sp)
	defer rec.Release()

	require.EqualValues(t, 404, rec.Column(spHTTPStatus).(*array.Int64).Value(0))
	require.EqualValues(t, 12, rec.Column(spInputTokens).(*array.Int64).Value(0))
	require.True(t, rec.Column(spOutputTokens).IsNull(0))
	require.Equal(t, "s1", rec.Column(spSession).(*array.String).Value(0))
	require.Equal(t, "/x", rec.Column(spHTTPRoute).(*array.String).Value(0))
	require.True(t, rec.Column(spHTTPMethod).IsNull(0))
	require.True(t, rec.Column(spModel).IsNull(0))

	// Copied, not moved.
	m := rec.Column(spAttrs).(*array.Map)
	require.EqualValues(t, 5, m.Offsets()[1]-m.Offsets()[0])
}

func TestSpanMinMaxFallsBackToReceivedAt(t *testing.T) {
	recvd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	started := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	b := NewSpansBuilder(memory.NewGoAllocator())
	defer b.Release()

	a := ptrace.NewSpan()
	a.SetStartTimestamp(pcommon.NewTimestampFromTime(started))
	b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), a, recvd)
	b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), ptrace.NewSpan(), recvd)

	require.Equal(t, 2, b.Len())
	require.Positive(t, b.EstimatedBytes())
	minT, maxT := b.MinMaxTime()
	require.True(t, started.Equal(minT))
	require.True(t, recvd.Equal(maxT))
}
