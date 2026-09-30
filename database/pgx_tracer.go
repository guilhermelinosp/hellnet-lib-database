package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type pgxSpanKey struct{}

// pgxTracer owns driver-level statement spans. It intentionally does not
// record arguments; SQL text is omitted when HideQueryArgs is enabled.
type pgxTracer struct {
	obs     observability
	options Options
}

func (t *pgxTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	operation, table := statementName(data.SQL)
	name := spanNameFor(operation, table)
	if operation == "" {
		operation = "query"
	}
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "postgresql"),
		attribute.String("db.operation.name", operation),
		attribute.String("db.namespace", t.options.Database),
		attribute.String("server.address", t.options.Host),
		attribute.Int("server.port", t.options.Port),
	}
	if table != "" {
		attrs = append(attrs, attribute.String("db.collection.name", table))
	}
	if !t.options.HideQueryArgs {
		attrs = append(attrs, attribute.String("db.query.text", data.SQL))
	}
	ctx, span := t.obs.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	return context.WithValue(ctx, pgxSpanKey{}, span)
}

func (t *pgxTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(pgxSpanKey{}).(trace.Span)
	if !ok {
		return
	}
	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetStatus(codes.Error, data.Err.Error())
	}
	span.End()
}

var _ pgx.QueryTracer = (*pgxTracer)(nil)
