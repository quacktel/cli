package tools

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

var severities = map[string]int{"trace": 1, "debug": 5, "info": 9, "warn": 13, "error": 17, "fatal": 21}

type ListServicesInput struct {
	Window
}

func (r *Runner) ListServices(ctx context.Context, in ListServicesInput) (Rows, error) {
	since, until, err := r.window(in.Window)
	if err != nil {
		return Rows{}, err
	}

	q := newQuery(`SELECT service_name AS service, sum(logs)::BIGINT AS logs, sum(spans)::BIGINT AS spans, min(ts) AS first_seen, max(ts) AS last_seen FROM (
SELECT service_name, 1 AS logs, 0 AS spans, time AS ts FROM logs WHERE time >= ? AND time <= ?
UNION ALL
SELECT service_name, 0 AS logs, 1 AS spans, start_time AS ts FROM spans WHERE start_time >= ? AND start_time <= ?
) GROUP BY service_name ORDER BY service_name`, since, until, since, until)
	res, err := r.run(ctx, q, maxLimit)
	if err != nil {
		return Rows{}, fmt.Errorf("failed to list services: %w", err)
	}

	return toRows(res), nil
}

type SearchLogsInput struct {
	Common
	MinSeverity string            `json:"min_severity,omitempty" jsonschema:"Minimum severity: a number 1-24 or one of trace, debug, info, warn, error, fatal."`
	Text        string            `json:"text,omitempty" jsonschema:"Case-insensitive substring of the log body."`
	Regex       string            `json:"regex,omitempty" jsonschema:"RE2 regular expression matched against the log body."`
	EventName   string            `json:"event_name,omitempty" jsonschema:"Exact event name."`
	TraceID     string            `json:"trace_id,omitempty" jsonschema:"Trace ID as 32 hex characters."`
	SessionID   string            `json:"session_id,omitempty" jsonschema:"Exact session ID."`
	Attrs       map[string]string `json:"attrs,omitempty" jsonschema:"Attribute key to exact value pairs that must all match."`
}

func parseSeverity(s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		if n < 1 || n > 24 {
			return 0, inputErr("min_severity %d is outside 1-24", n)
		}

		return n, nil
	}
	n, ok := severities[strings.ToLower(s)]
	if !ok {
		return 0, inputErr("min_severity %q must be 1-24 or one of trace, debug, info, warn, error, fatal", s)
	}

	return n, nil
}

func (r *Runner) SearchLogs(ctx context.Context, in SearchLogsInput) (Rows, error) {
	since, until, err := r.window(in.Window)
	if err != nil {
		return Rows{}, err
	}
	svc, err := r.scope(in.Service)
	if err != nil {
		return Rows{}, err
	}

	q := newQuery(`SELECT time, service_name, severity_number, severity_text, event_name, session_id, body, trace_id, span_id, attrs FROM logs WHERE time >= ? AND time <= ?`, since, until)
	if svc != "" {
		q.add("AND service_name = ?", svc)
	}
	if in.MinSeverity != "" {
		n, err := parseSeverity(in.MinSeverity)
		if err != nil {
			return Rows{}, err
		}
		q.add("AND severity_number >= ?", n)
	}
	if in.Text != "" {
		q.add("AND body ILIKE '%' || ? || '%'", in.Text)
	}
	if in.Regex != "" {
		// DuckDB uses RE2, whose syntax Go's regexp shares.
		if _, err := regexp.Compile(in.Regex); err != nil {
			return Rows{}, inputErr("regex does not compile: %v", err)
		}
		q.add("AND regexp_matches(body, ?)", in.Regex)
	}
	if in.EventName != "" {
		q.add("AND event_name = ?", in.EventName)
	}
	if in.TraceID != "" {
		id, err := validTraceID(in.TraceID)
		if err != nil {
			return Rows{}, err
		}
		q.add("AND trace_id = ?", id)
	}
	if in.SessionID != "" {
		q.add("AND session_id = ?", in.SessionID)
	}
	q.attrs(in.Attrs)
	q.add("ORDER BY time DESC")

	res, err := r.run(ctx, q, clampLimit(in.Limit))
	if err != nil {
		return Rows{}, fmt.Errorf("failed to search logs: %w", err)
	}

	return toRows(res), nil
}

var (
	spanKinds    = []string{"internal", "server", "client", "producer", "consumer", "unspecified"}
	spanStatuses = []string{"unset", "ok", "error"}
)

type FindSpansInput struct {
	Common
	Name          string            `json:"name,omitempty" jsonschema:"Case-insensitive substring of the span name."`
	Kind          string            `json:"kind,omitempty" jsonschema:"Span kind: internal, server, client, producer, consumer or unspecified."`
	Status        string            `json:"status,omitempty" jsonschema:"Span status: unset, ok or error."`
	HTTPRoute     string            `json:"http_route,omitempty" jsonschema:"Exact http.route value."`
	MinDurationMS int64             `json:"min_duration_ms,omitempty" jsonschema:"Only spans at least this many milliseconds long."`
	Attrs         map[string]string `json:"attrs,omitempty" jsonschema:"Attribute key to exact value pairs that must all match."`
}

func (r *Runner) FindSpans(ctx context.Context, in FindSpansInput) (Rows, error) {
	since, until, err := r.window(in.Window)
	if err != nil {
		return Rows{}, err
	}
	svc, err := r.scope(in.Service)
	if err != nil {
		return Rows{}, err
	}
	if in.Kind != "" && !slices.Contains(spanKinds, in.Kind) {
		return Rows{}, inputErr("kind %q must be one of %s", in.Kind, strings.Join(spanKinds, ", "))
	}
	if in.Status != "" && !slices.Contains(spanStatuses, in.Status) {
		return Rows{}, inputErr("status %q must be one of %s", in.Status, strings.Join(spanStatuses, ", "))
	}
	if in.MinDurationMS < 0 {
		return Rows{}, inputErr("min_duration_ms must not be negative")
	}

	q := newQuery("SELECT "+spanColumns+" FROM spans WHERE start_time >= ? AND start_time <= ?", since, until)
	if svc != "" {
		q.add("AND service_name = ?", svc)
	}
	if in.Name != "" {
		q.add("AND name ILIKE '%' || ? || '%'", in.Name)
	}
	if in.Kind != "" {
		q.add("AND kind = ?", in.Kind)
	}
	if in.Status != "" {
		q.add("AND status_code = ?", in.Status)
	}
	if in.HTTPRoute != "" {
		q.add("AND http_route = ?", in.HTTPRoute)
	}
	if in.MinDurationMS > 0 {
		q.add("AND duration_ns >= ?", in.MinDurationMS*int64(time.Millisecond))
	}
	q.attrs(in.Attrs)
	q.add("ORDER BY duration_ns DESC NULLS LAST")

	res, err := r.run(ctx, q, clampLimit(in.Limit))
	if err != nil {
		return Rows{}, fmt.Errorf("failed to find spans: %w", err)
	}

	return toRows(res), nil
}

const spanColumns = `trace_id, span_id, parent_span_id, name, kind, status_code, status_message, duration_ns, start_time, service_name, http_request_method, http_route, http_response_status_code, gen_ai_request_model, gen_ai_usage_input_tokens, gen_ai_usage_output_tokens, session_id`
