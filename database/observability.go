package database

import (
	"context"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-database/database"
	modulePath           = "github.com/guilhermelinosp/hellnet-lib-database"
)

type observability struct {
	inst              instrument.Instrumentation
	tracer            trace.Tracer
	meter             metric.Meter
	logger            instrument.Logger
	operationDuration metric.Float64Histogram
	transactions      metric.Int64Counter
	retries           metric.Int64Counter
}

func newObservability(inst instrument.Instrumentation) observability {
	if inst == nil {
		inst = instrument.Noop()
	}
	s := instrument.NewScope(inst, instrumentationScope, modulePath)
	return observability{
		inst:              inst,
		tracer:            s.Tracer,
		meter:             s.Meter,
		logger:            s.Logger,
		operationDuration: s.Float64Histogram("db.client.operation.duration", metric.WithUnit("s")),
		transactions:      s.Int64Counter("hellnet.db.transactions", metric.WithUnit("{operation}")),
		retries:           s.Int64Counter("hellnet.db.retries", metric.WithUnit("{operation}")),
	}
}

func (o observability) observeTransaction(ctx context.Context, result string) {
	instrument.Observe(ctx, o.transactions, nil, time.Time{}, attribute.String("result", result))
}

func (o observability) observeOperation(ctx context.Context, operation, result string, started time.Time) {
	instrument.Observe(ctx, nil, o.operationDuration, started,
		attribute.String("db.system.name", "postgresql"),
		attribute.String("db.operation.name", operation),
		attribute.String("result", result))
}
