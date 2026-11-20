package table

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	SchemaVersionKey = "quacktel.schema_version"
	SchemaVersion    = "1"

	unknownService = "unknown_service"
	fixedRowBytes  = 8 + 8 + 4 // time, observed_time, severity_number
)

// No TimeZone, so DuckDB reads TIMESTAMP_NS (values are UTC by convention); a tz-aware column becomes TIMESTAMPTZ_NS, which duckdb-go cannot scan.
var timeType = &arrow.TimestampType{Unit: arrow.Nanosecond}

func LogsSchema() *arrow.Schema {
	meta := arrow.NewMetadata([]string{SchemaVersionKey}, []string{SchemaVersion})
	str := arrow.BinaryTypes.String
	attrs := arrow.MapOf(str, str)

	return arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: timeType},
		{Name: "observed_time", Type: timeType, Nullable: true},
		{Name: "service_name", Type: str, Nullable: true},
		{Name: "severity_number", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "severity_text", Type: str, Nullable: true},
		{Name: "event_name", Type: str, Nullable: true},
		{Name: "session_id", Type: str, Nullable: true},
		{Name: "body", Type: str, Nullable: true},
		{Name: "trace_id", Type: str, Nullable: true},
		{Name: "span_id", Type: str, Nullable: true},
		{Name: "scope_name", Type: str, Nullable: true},
		{Name: "scope_version", Type: str, Nullable: true},
		{Name: "resource_attrs", Type: attrs, Nullable: true},
		{Name: "attrs", Type: attrs, Nullable: true},
	}, &meta)
}

// Column indexes in LogsSchema order.
const (
	colTime = iota
	colObserved
	colService
	colSeverityNumber
	colSeverityText
	colEvent
	colSession
	colBody
	colTraceID
	colSpanID
	colScopeName
	colScopeVersion
	colResourceAttrs
	colAttrs
)

type LogsBuilder struct {
	rb    *array.RecordBuilder
	n     int
	bytes int64
	min   time.Time
	max   time.Time
}

func NewLogsBuilder(mem memory.Allocator) *LogsBuilder {
	return &LogsBuilder{rb: array.NewRecordBuilder(mem, LogsSchema())}
}

func (b *LogsBuilder) Len() int { return b.n }

func (b *LogsBuilder) EstimatedBytes() int64 { return b.bytes }

func (b *LogsBuilder) MinMaxTime() (time.Time, time.Time) { return b.min, b.max }

func (b *LogsBuilder) Append(res pcommon.Resource, scope pcommon.InstrumentationScope, rec plog.LogRecord, receivedAt time.Time) {
	ts := rec.Timestamp()
	if ts == 0 {
		ts = rec.ObservedTimestamp()
	}
	if ts == 0 {
		ts = pcommon.NewTimestampFromTime(receivedAt)
	}
	t := ts.AsTime()
	if b.n == 0 || t.Before(b.min) {
		b.min = t
	}
	if b.n == 0 || t.After(b.max) {
		b.max = t
	}

	f := b.rb.Fields()
	f[colTime].(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixNano()))
	if rec.ObservedTimestamp() == 0 {
		f[colObserved].AppendNull()
	} else {
		f[colObserved].(*array.TimestampBuilder).Append(arrow.Timestamp(rec.ObservedTimestamp().AsTime().UnixNano()))
	}
	b.bytes += fixedRowBytes

	b.str(f[colService], ServiceName(res))
	f[colSeverityNumber].(*array.Int32Builder).Append(int32(rec.SeverityNumber()))
	b.str(f[colSeverityText], rec.SeverityText())

	event := rec.EventName()
	if event == "" {
		if v, ok := rec.Attributes().Get("event.name"); ok {
			event = v.AsString()
		}
	}
	b.str(f[colEvent], event)

	session := ""
	if v, ok := rec.Attributes().Get("session.id"); ok {
		session = v.AsString()
	}
	b.str(f[colSession], session)

	body := ""
	if rec.Body().Type() != pcommon.ValueTypeEmpty {
		body = rec.Body().AsString()
	}
	b.str(f[colBody], body)

	b.str(f[colTraceID], hexID(rec.TraceID().IsEmpty(), rec.TraceID().String()))
	b.str(f[colSpanID], hexID(rec.SpanID().IsEmpty(), rec.SpanID().String()))
	b.str(f[colScopeName], scope.Name())
	b.str(f[colScopeVersion], scope.Version())
	b.attrs(f[colResourceAttrs], res.Attributes())
	b.attrs(f[colAttrs], rec.Attributes())
	b.n++
}

// NewRecord resets the builder so it can be reused for the next flush.
func (b *LogsBuilder) NewRecord() arrow.RecordBatch {
	b.n = 0
	b.bytes = 0
	b.min, b.max = time.Time{}, time.Time{}

	return b.rb.NewRecordBatch()
}

func (b *LogsBuilder) Release() { b.rb.Release() }

// ServiceName returns the resource service.name, or unknown_service when missing or empty.
func ServiceName(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get("service.name"); ok {
		if s := v.AsString(); s != "" {
			return s
		}
	}

	return unknownService
}

func hexID(empty bool, s string) string {
	if empty {
		return ""
	}

	return s
}

func (b *LogsBuilder) str(fb array.Builder, s string) {
	if s == "" {
		fb.AppendNull()

		return
	}
	fb.(*array.StringBuilder).Append(s)
	b.bytes += int64(len(s))
}

func (b *LogsBuilder) attrs(fb array.Builder, m pcommon.Map) {
	mb := fb.(*array.MapBuilder)
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	m.Range(func(k string, v pcommon.Value) bool {
		val := v.AsString()
		kb.Append(k)
		ib.Append(val)
		b.bytes += int64(len(k) + len(val))

		return true
	})
}
