// Command otel-go-app sends one synthetic trace to the OTLP endpoint named by OTEL_EXPORTER_OTLP_ENDPOINT.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "otel-go-app:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return fmt.Errorf("failed to create exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(semconv.ServiceName("otel-go-app"))),
	)
	defer func() {
		if serr := tp.Shutdown(ctx); serr != nil {
			err = errors.Join(err, fmt.Errorf("failed to export spans: %w", serr))
		}
	}()

	tracer := tp.Tracer("otel-go-app")
	ctx, root := tracer.Start(ctx, "GET /items", trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", "GET"),
			attribute.String("http.route", "/items"),
			attribute.Int("http.response.status_code", 200),
		))
	defer root.End()

	_, chat := tracer.Start(ctx, "chat test-model", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("gen_ai.request.model", "test-model"),
			attribute.Int("gen_ai.usage.input_tokens", 12),
			attribute.Int("gen_ai.usage.output_tokens", 34),
			attribute.String("session.id", "sess-go-1"),
			attribute.String("auth_token", "tok-not-real"),
		))
	chat.AddEvent("retry", trace.WithAttributes(attribute.Int("attempt", 1)))
	chat.End()

	_, cache := tracer.Start(ctx, "load-cache", trace.WithSpanKind(trace.SpanKindInternal))
	cache.RecordError(errors.New("cache miss"))
	cache.SetStatus(codes.Error, "cache miss")
	cache.End()

	return nil
}
