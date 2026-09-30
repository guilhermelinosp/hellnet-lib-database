## Unreleased

- `New` and `MustNew` now take an `instrument.Instrumentation` (for example a
  `*telemetry.Telemetry`, or nil) instead of the legacy `telemetry.Client` and
  are no longer deprecated; `New(ctx, tel)` is env-first. `OpenFromEnv` is
  deprecated in favor of `New`.

- Added `WithInstrumentation` to `NewWithOptions` and instrumentation scope/provider initialization using telemetry v1.9.1.
- Deprecated constructor telemetry-client paths in favor of the instrument contract.
- Added ctx-first `ExecuteContext`, typed query context functions, connection/transaction context variants, and caller-parented database spans.
- Compensation rollback now derives from `context.WithoutCancel` with the command timeout.
- Added `db.client.operation.duration` histograms for context-first execute, query and scalar operations.
- Added optional `ContextQueryHook` for trace-correlated query hooks without changing `QueryHook`.
- Added `PingContext(ctx)`; the former `Ping()` uses the construction context and is deprecated.
- Context-first execute, query and scalar spans now record returned errors and set OTel error status.
- `QueryRowContext` now emits operation-duration metrics and OTel error status consistently with other ctx-first operations.
- Migrated slow-query and hook-panic logs to the instrumentation contract logger.
- Migrated constructor and transaction rollback-compensation logs to the instrumentation contract logger.
- Migrated LISTEN/reconnect logs to the instrumentation contract; reconnect tests now assert contract log emission.
- Migrated retry and command-timeout warning logs to the instrumentation contract; production database code no longer imports `log/slog`.
- Added pgx driver query tracing for pooled and SQLX-backed statements, with client spans and no query arguments.
- Added observable pool gauges `db.client.connection.count`, `db.client.connection.max` and `db.client.connection.wait_time`; callbacks are unregistered on `Close`.
- Added `hellnet.db.transactions` counter with `result=commit|rollback|panic`.
- Added `hellnet.db.retries` counter for transient retry attempts.
