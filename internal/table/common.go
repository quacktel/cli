package table

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	SchemaVersionKey = "quacktel.schema_version"
	SchemaVersion    = "1"
	SignalKey        = "quacktel.signal"

	unknownService = "unknown_service"
)

// No TimeZone, so DuckDB reads TIMESTAMP_NS (values are UTC by convention); a tz-aware column becomes TIMESTAMPTZ_NS, which duckdb-go cannot scan.
var timeType = &arrow.TimestampType{Unit: arrow.Nanosecond}

// base holds the row accounting and column helpers shared by the signal builders.
type base struct {
	rb    *array.RecordBuilder
	n     int
	bytes int64
	min   time.Time
	max   time.Time
}

func (b *base) Len() int { return b.n }

func (b *base) EstimatedBytes() int64 { return b.bytes }

func (b *base) MinMaxTime() (time.Time, time.Time) { return b.min, b.max }

// track records t in the min/max range; call it once per row, before n is incremented.
func (b *base) track(t time.Time) {
	if b.n == 0 || t.Before(b.min) {
		b.min = t
	}
	if b.n == 0 || t.After(b.max) {
		b.max = t
	}
}

// NewRecord resets the builder so it can be reused for the next flush.
func (b *base) NewRecord() arrow.RecordBatch {
	b.n = 0
	b.bytes = 0
	b.min, b.max = time.Time{}, time.Time{}

	return b.rb.NewRecordBatch()
}

func (b *base) Release() { b.rb.Release() }

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

func (b *base) str(fb array.Builder, s string) {
	if s == "" {
		fb.AppendNull()

		return
	}
	fb.(*array.StringBuilder).Append(s)
	b.bytes += int64(len(s))
}

func (b *base) attrs(fb array.Builder, m pcommon.Map) {
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
