package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func registerPoolMetrics(obs observability, pool *pgxpool.Pool) metric.Registration {
	used, err := obs.meter.Int64ObservableGauge("db.client.connection.count", metric.WithUnit("{connection}"))
	if err != nil {
		obs.logger.Error(context.TODO(), "database pool metric creation failed", "metric", "db.client.connection.count", "error", err)
		return nil
	}
	max, err := obs.meter.Int64ObservableGauge("db.client.connection.max", metric.WithUnit("{connection}"))
	if err != nil {
		obs.logger.Error(context.TODO(), "database pool metric creation failed", "metric", "db.client.connection.max", "error", err)
		return nil
	}
	wait, err := obs.meter.Float64ObservableGauge("db.client.connection.wait_time", metric.WithUnit("s"))
	if err != nil {
		obs.logger.Error(context.TODO(), "database pool metric creation failed", "metric", "db.client.connection.wait_time", "error", err)
		return nil
	}
	registration, err := obs.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stat := pool.Stat()
		observer.ObserveInt64(used, int64(stat.AcquiredConns()), metric.WithAttributes(attribute.String("db.client.connection.state", "used")))
		observer.ObserveInt64(used, int64(stat.IdleConns()), metric.WithAttributes(attribute.String("db.client.connection.state", "idle")))
		observer.ObserveInt64(max, int64(stat.MaxConns()))
		observer.ObserveFloat64(wait, stat.EmptyAcquireWaitTime().Seconds())
		return nil
	}, used, max, wait)
	if err != nil {
		obs.logger.Error(context.TODO(), "database pool metric callback registration failed", "error", err)
		return nil
	}
	return registration
}
