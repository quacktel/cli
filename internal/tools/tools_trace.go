package tools

import (
	"context"
	"fmt"
)

const (
	maxTraceSpans = 1000
	maxTraceLogs  = 500
	topAttrKeys   = 30
)

type GetTraceInput struct {
	TraceID string `json:"trace_id" jsonschema:"Trace ID as 32 hex characters."`
	Service string `json:"service,omitempty" jsonschema:"Only include this service name."`
}

type TraceOutput struct {
	Spans     []map[string]any `json:"spans"`
	Logs      []map[string]any `json:"logs"`
	Truncated bool             `json:"truncated"`
	Note      string           `json:"note"`
}

func (r *Runner) GetTrace(ctx context.Context, in GetTraceInput) (TraceOutput, error) {
	id, err := validTraceID(in.TraceID)
	if err != nil {
		return TraceOutput{}, err
	}
	svc, err := r.scope(in.Service)
	if err != nil {
		return TraceOutput{}, err
	}

	sq := newQuery("SELECT "+spanColumns+", end_time, attrs, list_transform(events, lambda e: e.name) AS event_names FROM spans WHERE trace_id = ?", id)
	lq := newQuery("SELECT time, service_name, severity_number, severity_text, event_name, body, span_id, attrs FROM logs WHERE trace_id = ?", id)
	if svc != "" {
		sq.add("AND service_name = ?", svc)
		lq.add("AND service_name = ?", svc)
	}
	sq.add("ORDER BY start_time")
	lq.add("ORDER BY time")

	sres, err := r.run(ctx, sq, maxTraceSpans)
	if err != nil {
		return TraceOutput{}, fmt.Errorf("failed to get trace spans: %w", err)
	}
	lres, err := r.run(ctx, lq, maxTraceLogs)
	if err != nil {
		return TraceOutput{}, fmt.Errorf("failed to get trace logs: %w", err)
	}

	spans, logs := toRows(sres), toRows(lres)
	addDepths(spans.Rows)
	for _, s := range spans.Rows {
		if s["event_names"] == nil {
			s["event_names"] = []any{}
		}
	}

	return TraceOutput{Spans: spans.Rows, Logs: logs.Rows, Truncated: spans.Truncated || logs.Truncated, Note: untrustedNote}, nil
}

func addDepths(spans []map[string]any) {
	parents := make(map[string]string, len(spans))
	for _, s := range spans {
		id, _ := s["span_id"].(string)
		parent, _ := s["parent_span_id"].(string)
		parents[id] = parent
	}
	for _, s := range spans {
		id, _ := s["span_id"].(string)
		depth := 0
		for cur := parents[id]; ; cur = parents[cur] {
			if _, ok := parents[cur]; !ok {
				break
			}
			depth++
			// A walk longer than the span count means a parent cycle.
			if depth > len(spans) {
				depth = 0

				break
			}
		}
		s["depth"] = depth
	}
}

type SQLInput struct {
	Query string `json:"query" jsonschema:"A single SELECT statement over the logs and spans views."`
	Limit int    `json:"limit,omitempty" jsonschema:"Maximum rows to return. Defaults to 100, capped at 500."`
}

func (r *Runner) SQL(ctx context.Context, in SQLInput) (Rows, error) {
	if in.Query == "" {
		return Rows{}, inputErr("query is required")
	}
	res, err := r.Eng.Query(ctx, in.Query, clampLimit(in.Limit))
	if err != nil {
		if ctx.Err() != nil {
			return Rows{}, fmt.Errorf("query cancelled: %w", err)
		}

		// Guard rejections, syntax and permission errors all come from the caller's SQL, so the agent should see them.
		return Rows{}, fmt.Errorf("%w: %w", ErrInput, err)
	}

	return toRows(res), nil
}
