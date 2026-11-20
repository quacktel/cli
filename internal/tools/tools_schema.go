package tools

import (
	"context"
	"fmt"
)

type DescribeSchemaInput struct {
	Service   string `json:"service,omitempty" jsonschema:"Only count attribute keys for this service."`
	EventName string `json:"event_name,omitempty" jsonschema:"Only count log attribute keys for this event name; spans are unaffected."`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AttributeKey struct {
	Key     string `json:"key"`
	Count   int64  `json:"count"`
	Example string `json:"example"`
}

type SignalSchema struct {
	Columns          []Column       `json:"columns"`
	TopAttributeKeys []AttributeKey `json:"top_attribute_keys"`
}

type SchemaOutput struct {
	Logs      SignalSchema `json:"logs"`
	Spans     SignalSchema `json:"spans"`
	Truncated bool         `json:"truncated"`
	Note      string       `json:"note"`
}

func (r *Runner) DescribeSchema(ctx context.Context, in DescribeSchemaInput) (SchemaOutput, error) {
	svc, err := r.scope(in.Service)
	if err != nil {
		return SchemaOutput{}, err
	}
	since := r.now().Add(-defaultWindow).UTC()

	out := SchemaOutput{Note: untrustedNote}
	for _, sig := range []struct {
		dst  *SignalSchema
		desc string
		keys string
	}{
		{&out.Logs, "SELECT column_name, column_type FROM (DESCRIBE logs)", "SELECT unnest(map_keys(attrs)) AS key, attrs FROM logs WHERE time >= ?"},
		{&out.Spans, "SELECT column_name, column_type FROM (DESCRIBE spans)", "SELECT unnest(map_keys(attrs)) AS key, attrs FROM spans WHERE start_time >= ?"},
	} {
		cols, err := r.Eng.Query(ctx, sig.desc, maxLimit)
		if err != nil {
			return SchemaOutput{}, fmt.Errorf("failed to describe schema: %w", err)
		}
		sig.dst.Columns = make([]Column, 0, len(cols.Rows))
		for _, row := range cols.Rows {
			name, _ := row[0].(string)
			typ, _ := row[1].(string)
			sig.dst.Columns = append(sig.dst.Columns, Column{Name: name, Type: typ})
		}

		inner := newQuery(sig.keys, since)
		if svc != "" {
			inner.add("AND service_name = ?", svc)
		}
		if sig.dst == &out.Logs && in.EventName != "" {
			inner.add("AND event_name = ?", in.EventName)
		}
		q := newQuery("SELECT key, count(*)::BIGINT AS n, any_value(attrs[key]) AS example FROM ("+inner.sql.String()+") GROUP BY key ORDER BY n DESC, key", inner.args...)
		res, err := r.run(ctx, q, topAttrKeys)
		if err != nil {
			return SchemaOutput{}, fmt.Errorf("failed to list attribute keys: %w", err)
		}
		sig.dst.TopAttributeKeys = make([]AttributeKey, 0, len(res.Rows))
		for _, row := range res.Rows {
			k, _ := row[0].(string)
			n, _ := row[1].(int64)
			ex, _ := row[2].(string)
			sig.dst.TopAttributeKeys = append(sig.dst.TopAttributeKeys, AttributeKey{
				Key: truncateString(k, maxStringLen, &out.Truncated), Count: n, Example: truncateString(ex, maxExampleLen, &out.Truncated),
			})
		}
	}

	return out, nil
}
