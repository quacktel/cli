package table

import (
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// start_time, end_time, duration_ns and the three promoted int64 columns.
const spanFixedRowBytes = 8 * 6

func SpansSchema() *arrow.Schema {
	meta := arrow.NewMetadata([]string{SchemaVersionKey, SignalKey}, []string{SchemaVersion, "spans"})
	str := arrow.BinaryTypes.String
	i64 := arrow.PrimitiveTypes.Int64
	attrs := arrow.MapOf(str, str)
	events := arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "time", Type: timeType, Nullable: true},
		arrow.Field{Name: "name", Type: str, Nullable: true},
		arrow.Field{Name: "attrs", Type: attrs, Nullable: true},
	))
	links := arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "trace_id", Type: str, Nullable: true},
		arrow.Field{Name: "span_id", Type: str, Nullable: true},
		arrow.Field{Name: "trace_state", Type: str, Nullable: true},
		arrow.Field{Name: "attrs", Type: attrs, Nullable: true},
	))

	return arrow.NewSchema([]arrow.Field{
		{Name: "trace_id", Type: str},
		{Name: "span_id", Type: str},
		{Name: "parent_span_id", Type: str, Nullable: true},
		{Name: "trace_state", Type: str, Nullable: true},
		{Name: "name", Type: str, Nullable: true},
		{Name: "kind", Type: str, Nullable: true},
		{Name: "start_time", Type: timeType, Nullable: true},
		{Name: "end_time", Type: timeType, Nullable: true},
		{Name: "duration_ns", Type: i64, Nullable: true},
		{Name: "status_code", Type: str, Nullable: true},
		{Name: "status_message", Type: str, Nullable: true},
		{Name: "service_name", Type: str, Nullable: true},
		{Name: "scope_name", Type: str, Nullable: true},
		{Name: "scope_version", Type: str, Nullable: true},
		{Name: "session_id", Type: str, Nullable: true},
		{Name: "http_request_method", Type: str, Nullable: true},
		{Name: "http_response_status_code", Type: i64, Nullable: true},
		{Name: "http_route", Type: str, Nullable: true},
		{Name: "gen_ai_request_model", Type: str, Nullable: true},
		{Name: "gen_ai_usage_input_tokens", Type: i64, Nullable: true},
		{Name: "gen_ai_usage_output_tokens", Type: i64, Nullable: true},
		{Name: "resource_attrs", Type: attrs, Nullable: true},
		{Name: "attrs", Type: attrs, Nullable: true},
		{Name: "events", Type: events, Nullable: true},
		{Name: "links", Type: links, Nullable: true},
	}, &meta)
}

// Column indexes in SpansSchema order.
const (
	spTraceID = iota
	spSpanID
	spParentSpanID
	spTraceState
	spName
	spKind
	spStart
	spEnd
	spDuration
	spStatusCode
	spStatusMessage
	spService
	spScopeName
	spScopeVersion
	spSession
	spHTTPMethod
	spHTTPStatus
	spHTTPRoute
	spModel
	spInputTokens
	spOutputTokens
	spResourceAttrs
	spAttrs
	spEvents
	spLinks
)

type SpansBuilder struct{ base }

func NewSpansBuilder(mem memory.Allocator) *SpansBuilder {
	return &SpansBuilder{base{rb: array.NewRecordBuilder(mem, SpansSchema())}}
}

func (b *SpansBuilder) Append(res pcommon.Resource, scope pcommon.InstrumentationScope, span ptrace.Span, receivedAt time.Time) {
	start, end := span.StartTimestamp(), span.EndTimestamp()
	if start == 0 {
		b.track(receivedAt)
	} else {
		b.track(start.AsTime())
	}

	f := b.rb.Fields()
	b.mustStr(f[spTraceID], span.TraceID().String())
	b.mustStr(f[spSpanID], span.SpanID().String())
	b.str(f[spParentSpanID], hexID(span.ParentSpanID().IsEmpty(), span.ParentSpanID().String()))
	b.str(f[spTraceState], span.TraceState().AsRaw())
	b.str(f[spName], span.Name())
	b.str(f[spKind], kindName(span.Kind()))
	b.ts(f[spStart], start)
	b.ts(f[spEnd], end)
	if start == 0 || end == 0 || end < start {
		f[spDuration].AppendNull()
	} else {
		f[spDuration].(*array.Int64Builder).Append(end.AsTime().Sub(start.AsTime()).Nanoseconds())
	}
	b.str(f[spStatusCode], statusName(span.Status().Code()))
	b.str(f[spStatusMessage], span.Status().Message())
	b.str(f[spService], ServiceName(res))
	b.str(f[spScopeName], scope.Name())
	b.str(f[spScopeVersion], scope.Version())

	// Promoted keys are copied, so attrs stays the full record.
	a := span.Attributes()
	b.str(f[spSession], strAttr(a, "session.id"))
	b.str(f[spHTTPMethod], strAttr(a, "http.request.method"))
	b.int(f[spHTTPStatus], a, "http.response.status_code")
	b.str(f[spHTTPRoute], strAttr(a, "http.route"))
	b.str(f[spModel], strAttr(a, "gen_ai.request.model"))
	b.int(f[spInputTokens], a, "gen_ai.usage.input_tokens")
	b.int(f[spOutputTokens], a, "gen_ai.usage.output_tokens")
	b.bytes += spanFixedRowBytes

	b.attrs(f[spResourceAttrs], res.Attributes())
	b.attrs(f[spAttrs], a)
	b.events(f[spEvents].(*array.ListBuilder), span.Events())
	b.links(f[spLinks].(*array.ListBuilder), span.Links())
	b.n++
}

func (b *SpansBuilder) events(lb *array.ListBuilder, evs ptrace.SpanEventSlice) {
	lb.Append(true)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for i := range evs.Len() {
		ev := evs.At(i)
		sb.Append(true)
		b.ts(sb.FieldBuilder(0), ev.Timestamp())
		b.str(sb.FieldBuilder(1), ev.Name())
		b.attrs(sb.FieldBuilder(2), ev.Attributes())
		b.bytes += 8
	}
}

func (b *SpansBuilder) links(lb *array.ListBuilder, ls ptrace.SpanLinkSlice) {
	lb.Append(true)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for i := range ls.Len() {
		l := ls.At(i)
		sb.Append(true)
		b.str(sb.FieldBuilder(0), hexID(l.TraceID().IsEmpty(), l.TraceID().String()))
		b.str(sb.FieldBuilder(1), hexID(l.SpanID().IsEmpty(), l.SpanID().String()))
		b.str(sb.FieldBuilder(2), l.TraceState().AsRaw())
		b.attrs(sb.FieldBuilder(3), l.Attributes())
	}
}

// mustStr appends to a non-null column, so empty stays an empty string.
func (b *SpansBuilder) mustStr(fb array.Builder, s string) {
	fb.(*array.StringBuilder).Append(s)
	b.bytes += int64(len(s))
}

func (b *SpansBuilder) ts(fb array.Builder, ts pcommon.Timestamp) {
	if ts == 0 {
		fb.AppendNull()

		return
	}
	fb.(*array.TimestampBuilder).Append(arrow.Timestamp(ts.AsTime().UnixNano()))
}

// int appends an int attribute or a numeric string, and null for anything else.
func (b *SpansBuilder) int(fb array.Builder, m pcommon.Map, key string) {
	v, ok := m.Get(key)
	if !ok {
		fb.AppendNull()

		return
	}
	var n int64
	switch v.Type() {
	case pcommon.ValueTypeInt:
		n = v.Int()
	case pcommon.ValueTypeStr:
		p, err := strconv.ParseInt(v.Str(), 10, 64)
		if err != nil {
			fb.AppendNull()

			return
		}
		n = p
	case pcommon.ValueTypeEmpty, pcommon.ValueTypeDouble, pcommon.ValueTypeBool, pcommon.ValueTypeMap, pcommon.ValueTypeSlice, pcommon.ValueTypeBytes:
		fb.AppendNull()

		return
	}
	fb.(*array.Int64Builder).Append(n)
}

func strAttr(m pcommon.Map, key string) string {
	if v, ok := m.Get(key); ok {
		return v.AsString()
	}

	return ""
}

func kindName(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindInternal:
		return "internal"
	case ptrace.SpanKindServer:
		return "server"
	case ptrace.SpanKindClient:
		return "client"
	case ptrace.SpanKindProducer:
		return "producer"
	case ptrace.SpanKindConsumer:
		return "consumer"
	case ptrace.SpanKindUnspecified:
		return "unspecified"
	}

	return "unspecified"
}

func statusName(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeOk:
		return "ok"
	case ptrace.StatusCodeError:
		return "error"
	case ptrace.StatusCodeUnset:
		return "unset"
	}

	return "unset"
}
