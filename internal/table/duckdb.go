package table

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// DuckDBType maps the Arrow types used by the quacktel schemas to the DuckDB type read_parquet infers for them.
func DuckDBType(dt arrow.DataType) (string, error) {
	switch t := dt.(type) {
	case *arrow.StringType:
		return "VARCHAR", nil
	case *arrow.Int32Type:
		return "INTEGER", nil
	case *arrow.Int64Type:
		return "BIGINT", nil
	case *arrow.TimestampType:
		if t.Unit != arrow.Nanosecond || t.TimeZone != "" {
			return "", fmt.Errorf("unsupported arrow type %s", dt)
		}

		return "TIMESTAMP_NS", nil
	case *arrow.MapType:
		k, err := DuckDBType(t.KeyType())
		if err != nil {
			return "", err
		}
		v, err := DuckDBType(t.ItemType())
		if err != nil {
			return "", err
		}

		return "MAP(" + k + ", " + v + ")", nil
	case *arrow.ListType:
		e, err := DuckDBType(t.Elem())
		if err != nil {
			return "", err
		}

		return e + "[]", nil
	case *arrow.StructType:
		parts := make([]string, 0, t.NumFields())
		for _, f := range t.Fields() {
			ft, err := DuckDBType(f.Type)
			if err != nil {
				return "", err
			}
			parts = append(parts, quoteIdent(f.Name)+" "+ft)
		}

		return "STRUCT(" + strings.Join(parts, ", ") + ")", nil
	default:
		return "", fmt.Errorf("unsupported arrow type %s", dt)
	}
}

// DuckDBColumns returns a select list of typed NULLs, so a view over zero files still has the schema's columns.
func DuckDBColumns(s *arrow.Schema) (string, error) {
	cols := make([]string, 0, len(s.Fields()))
	for _, f := range s.Fields() {
		t, err := DuckDBType(f.Type)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", f.Name, err)
		}
		cols = append(cols, "NULL::"+t+" AS "+quoteIdent(f.Name))
	}

	return strings.Join(cols, ", "), nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
