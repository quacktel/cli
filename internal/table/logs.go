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
	fixedRowBytes = 8 + 8 + 4 // time, observed_time, severity_number
)

func LogsSchema() *arrow.Schema {
	meta := arrow.NewMetadata([]string{SchemaVersionKey, SignalKey}, []string{SchemaVersion, "logs"})
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

type LogsBuilder struct{ base }

func NewLogsBuilder(mem memory.Allocator) *LogsBuilder {
	return &LogsBuilder{base{rb: array.NewRecordBuilder(mem, LogsSchema())}}
}

func (b *LogsBuilder) Append(res pcommon.Resource, scope pcommon.InstrumentationScope, rec plog.LogRecord, receivedAt time.Time) {
	ts := rec.Timestamp()
	if ts == 0 {
		ts = rec.ObservedTimestamp()
	}
	if ts == 0 {
		ts = pcommon.NewTimestampFromTime(receivedAt)
	}
	t := ts.AsTime()
	b.track(t)

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
