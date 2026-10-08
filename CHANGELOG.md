## Unreleased

- `ListenWithReconnect` now survives a connection that never recovers (failover, restart, killed backend): after
  `ListenOptions.ReacquireAfter` (default 3) consecutive failed re-listen attempts on a `DB.Acquire` connection, it borrows a
  fresh pooled connection, issues `LISTEN` there and keeps delivering. The replacement is released by `stop()`; the original
  pinned `Conn` is still the caller's to `Close`. Standalone `Connect` connections keep retrying on the same session.

- A `nil` `context.Context` is no longer replaced by `context.Background()` (in
  `New`, `NewSQLX`, `BeginTx`, the `*Context` query helpers and the stored
  construction context): callers must pass a real context, as the Go convention
  requires, instead of silently losing cancelation and tracing.

- `Conn.Listen`/`ListenWithReconnect` no longer open a `db.execute` span for the
  initial `LISTEN`: it ran through the legacy `Conn.Execute` (a span via the
  telemetry client), leaving one orphan root trace per (re)connect.

- Driver statement spans use low-cardinality names: `db.insert`, `db.select`,
  `db.update`, `db.delete` and `db.transaction` (BEGIN/COMMIT/ROLLBACK), or
  `db.query` when a statement cannot be classified. A `WITH` (CTE) statement is
  named after its main statement. The table stays in `db.collection.name` and
  the verb in `db.operation.name`.

- The API spans `db.execute`, `db.query` and `db.scalar` are now `Internal`
  instead of `Client`: they wrap the retry attempts, and each attempt already has
  its own driver (`Client`) statement span, so a call no longer counts twice as a
  client in service graphs and span metrics.
- `LISTEN` and `UNLISTEN` no longer create statement spans (session control, one
  root trace per reconnect).

- Driver-level statement spans are named after the SQL operation and table
  (`INSERT outbox_events`, `SELECT users`, `BEGIN`, `COMMIT`) instead of
  `db.query`, and carry `db.operation.name` and `db.collection.name`; a
  statement that cannot be classified keeps `db.query`.

- `New` resolves its instrumentation with `instrument.Resolve`: a nil value or a nil pointer
  (for example a nil `*telemetry.Telemetry`) disables telemetry instead of panicking.

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
