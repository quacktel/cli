package table

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestSchemaVersion(t *testing.T) {
	v, ok := LogsSchema().Metadata().GetValue(SchemaVersionKey)
	require.True(t, ok)
	require.Equal(t, "1", v)
}

func TestAppendFallbacks(t *testing.T) {
	recvd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	observed := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	b := NewLogsBuilder(memory.NewGoAllocator())
	defer b.Release()

	res := pcommon.NewResource()
	scope := pcommon.NewInstrumentationScope()
	rec := plog.NewLogRecord()
	rec.Attributes().PutStr("event.name", "legacy.event")
	b.Append(res, scope, rec, recvd)

	rec2 := plog.NewLogRecord()
	rec2.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))
	b.Append(res, scope, rec2, recvd)

	require.Equal(t, 2, b.Len())
	require.Positive(t, b.EstimatedBytes())
	minT, maxT := b.MinMaxTime()
	require.True(t, observed.Equal(minT))
	require.True(t, recvd.Equal(maxT))

	batch := b.NewRecord()
	defer batch.Release()
	require.Zero(t, b.Len())
	require.Zero(t, b.EstimatedBytes())

	times := batch.Column(colTime).(*array.Timestamp)
	require.EqualValues(t, recvd.UnixNano(), times.Value(0))
	require.EqualValues(t, observed.UnixNano(), times.Value(1))
	require.Equal(t, unknownService, batch.Column(colService).(*array.String).Value(0))
	require.Equal(t, "legacy.event", batch.Column(colEvent).(*array.String).Value(0))
	require.True(t, batch.Column(colEvent).IsNull(1))
	require.True(t, batch.Column(colBody).IsNull(0))
	require.True(t, batch.Column(colObserved).IsNull(0))
	require.False(t, batch.Column(colObserved).IsNull(1))
}
