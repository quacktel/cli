package table

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestDuckDBTypeAllColumns(t *testing.T) {
	for _, s := range []*arrow.Schema{LogsSchema(), SpansSchema()} {
		for _, f := range s.Fields() {
			_, err := DuckDBType(f.Type)
			require.NoError(t, err, f.Name)
		}
	}
}

func TestDuckDBTypeNested(t *testing.T) {
	f, ok := SpansSchema().FieldsByName("events")
	require.True(t, ok)
	got, err := DuckDBType(f[0].Type)
	require.NoError(t, err)
	require.Equal(t, `STRUCT("time" TIMESTAMP_NS, "name" VARCHAR, "attrs" MAP(VARCHAR, VARCHAR))[]`, got)

	f, ok = SpansSchema().FieldsByName("links")
	require.True(t, ok)
	got, err = DuckDBType(f[0].Type)
	require.NoError(t, err)
	require.Equal(t, `STRUCT("trace_id" VARCHAR, "span_id" VARCHAR, "trace_state" VARCHAR, "attrs" MAP(VARCHAR, VARCHAR))[]`, got)
}

func TestDuckDBTypeQuotesFieldNames(t *testing.T) {
	got, err := DuckDBType(arrow.StructOf(arrow.Field{Name: `a"b`, Type: arrow.BinaryTypes.String}))
	require.NoError(t, err)
	require.Equal(t, `STRUCT("a""b" VARCHAR)`, got)
}

func TestDuckDBTypeUnsupported(t *testing.T) {
	_, err := DuckDBType(arrow.FixedWidthTypes.Boolean)
	require.Error(t, err)
	_, err = DuckDBType(&arrow.TimestampType{Unit: arrow.Microsecond})
	require.Error(t, err)
	_, err = DuckDBType(&arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	require.Error(t, err)
	_, err = DuckDBColumns(arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.FixedWidthTypes.Boolean}}, nil))
	require.Error(t, err)
}

func writeOneRow(t *testing.T, signal string) string {
	t.Helper()

	recvd := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var rec arrow.RecordBatch
	if signal == "logs" {
		b := NewLogsBuilder(memory.NewGoAllocator())
		defer b.Release()
		b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), plog.NewLogRecord(), recvd)
		rec = b.NewRecord()
	} else {
		b := NewSpansBuilder(memory.NewGoAllocator())
		defer b.Release()
		b.Append(pcommon.NewResource(), pcommon.NewInstrumentationScope(), ptrace.NewSpan(), recvd)
		rec = b.NewRecord()
	}
	defer rec.Release()

	var buf bytes.Buffer
	w, err := pqarrow.NewFileWriter(rec.Schema(), &buf, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())

	path := filepath.Join(t.TempDir(), signal+".parquet")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0o600))

	return path
}

func describe(t *testing.T, db *sql.DB, query string) [][2]string {
	t.Helper()

	rows, err := db.QueryContext(t.Context(), "DESCRIBE "+query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out [][2]string
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		out = append(out, [2]string{vals[0].(string), vals[1].(string)})
	}
	require.NoError(t, rows.Err())

	return out
}

func TestDuckDBColumnsMatchParquet(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	for signal, schema := range map[string]*arrow.Schema{"logs": LogsSchema(), "spans": SpansSchema()} {
		cols, err := DuckDBColumns(schema)
		require.NoError(t, err)
		path := writeOneRow(t, signal)

		got := describe(t, db, "SELECT "+cols+" LIMIT 0")
		want := describe(t, db, "SELECT * FROM read_parquet('"+path+"')")
		require.Equal(t, want, got, signal)
		require.Len(t, got, len(schema.Fields()))
		for _, c := range got {
			if c[0] == "events" || c[0] == "links" || c[0] == "time" {
				t.Logf("%s.%s %s", signal, c[0], c[1])
			}
		}
	}
}
