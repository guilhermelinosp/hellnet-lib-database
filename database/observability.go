package database

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-database/database"

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
	version := moduleVersion()
	logger := inst.Logger(instrumentationScope)
	meter := inst.MeterProvider().Meter(instrumentationScope, metric.WithInstrumentationVersion(version))
	duration, err := meter.Float64Histogram("db.client.operation.duration", metric.WithUnit("s"))
	if err != nil {
		logger.Error(context.TODO(), "database metric creation failed", "metric", "db.client.operation.duration", "error", err)
		duration, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Float64Histogram("db.client.operation.duration")
	}
	transactions, err := meter.Int64Counter("hellnet.db.transactions", metric.WithUnit("{operation}"))
	if err != nil {
		logger.Error(context.TODO(), "database metric creation failed", "metric", "hellnet.db.transactions", "error", err)
		transactions, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Int64Counter("hellnet.db.transactions")
	}
	retries, err := meter.Int64Counter("hellnet.db.retries", metric.WithUnit("{operation}"))
	if err != nil {
		logger.Error(context.TODO(), "database metric creation failed", "metric", "hellnet.db.retries", "error", err)
		retries, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Int64Counter("hellnet.db.retries")
	}
	return observability{inst: inst,
		tracer: inst.TracerProvider().Tracer(instrumentationScope, trace.WithInstrumentationVersion(version)),
		meter:  meter, logger: logger, operationDuration: duration, transactions: transactions, retries: retries}
}

func (o observability) observeTransaction(ctx context.Context, result string) {
	if o.transactions == nil {
		return
	}
	o.transactions.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
}

func (o observability) observeOperation(ctx context.Context, operation, result string, started time.Time) {
	o.operationDuration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(
		attribute.String("db.system.name", "postgresql"),
		attribute.String("db.operation.name", operation),
		attribute.String("result", result)))
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/guilhermelinosp/hellnet-lib-database" && dep.Version != "" {
			return dep.Version
		}
	}
	return "unknown"
}
