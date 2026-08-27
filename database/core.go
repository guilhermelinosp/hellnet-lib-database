package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// runner is the minimal statement-execution surface shared by connection pools
// (*pgxpool.Pool) and transactions (pgx.Tx satisfies it natively). Keeping it
// this small is what lets one generic core serve both auto-commit and
// transactional paths without adapters.
type runner interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// conn carries the backend (pool or live transaction), the effective options
// and the context captured ONCE at construction time (New/Connect) — every
// operation derives its per-command context from it internally, so no public
// method takes a context.Context.
//
// hooks é um ponteiro COMPARTILHADO de registro: DB, Conn (Acquire/Connect)
// e Tx copiam a struct conn por valor, então um slice embutido nunca veria
// hooks adicionados em runtime (DB.EnableMetrics). O ponteiro compartilhado
// resolve isso e mantém a leitura em dispatch race-free.
type conn struct {
	r     runner
	o     Options
	ctx   context.Context
	hooks *hookRegistry
}

// newConn monta um conn raiz (New/Connect) com o registro de hooks derivado
// das Options. Conexões FILHAS (Tx, Conn adquirida) devem usar conn.derive,
// que compartilha o mesmo registro — inclusive hooks adicionados depois.
func newConn(r runner, o Options, ctx context.Context) conn {
	return conn{r: r, o: o, ctx: ctx, hooks: newHookRegistry(o)}
}

// derive cria um conn filho (Tx de Begin/Transactional, Conn de Acquire)
// herdando opções/base e COMPARTILHANDO o registry de hooks do pai: métricas
// habilitadas tardiamente no DB alcançam todas as transações/conexões novas.
func (c *conn) derive(r runner) conn {
	return conn{r: r, o: c.o, ctx: c.base(), hooks: c.hooks}
}

// observed devolve o MetricsCollector observando este conn (ou nil).
// Nil-receiver-safe por construção: quem chama não precisa checar.
func (c *conn) observed() *MetricsCollector {
	if c.hooks == nil {
		return nil
	}
	return c.hooks.observed()
}

// fireBefore chama BeforeHook de todos os hooks ANTES da execução e antes do
// derive do contexto (ordem: pre-hook → timeout wrap → runner). Os hooks só
// observam; o contrato documentado diz que não devem mutar sql/args.
func (c *conn) fireBefore(op, sql string, args []any) {
	c.eachHook(func(h QueryHook) {
		h.BeforeHook(QueryInfo{Op: op, SQL: sql, Args: visibleArgs(c.o, args)})
	})
}

// fireAfter chama AfterHook de todos os hooks com o resultado completo —
// inclusive nos caminhos de erro de contexto (cancelamento/deadline). É
// always-called: um after para cada before, salvo pânico do próprio hook,
// que é contido por recover INDIVIDUAL sem afetar os demais.
func (c *conn) fireAfter(op, sql string, args []any, d time.Duration, err error, rows int64) {
	vargs := visibleArgs(c.o, args)
	c.eachHook(func(h QueryHook) {
		h.AfterHook(QueryInfo{Op: op, SQL: sql, Args: vargs, Duration: d, Err: err, Rows: rows})
	})
}

// eachHook itera sobre os hooks disparando fn sob recover individual: um hook
// em pânico vira slog.Warn e NUNCA quebra a query nem impede os demais hooks
// (nem a fase After da operação). Contrato documentado: hooks NÃO devem entrar
// em pânico; isto é defesa em profundidade da biblioteca.
func (c *conn) eachHook(fn func(QueryHook)) {
	if c.hooks == nil {
		return
	}
	for _, h := range c.hooks.snapshot() {
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Warn("database: query hook panicked (recovered)",
						"hook", fmt.Sprintf("%T", h), "panic", r)
				}
			}()
			fn(h)
		}()
	}
}

// visibleArgs aplica Options.HideQueryArgs: quando ativo, os args nunca
// alcançam os hooks (PII/segredos cortados antes de qualquer observabilidade).
func visibleArgs(o Options, args []any) []any {
	if o.HideQueryArgs {
		return nil
	}
	return args
}

// base returns the construction-time context, falling back to
// context.Background() when none was stored. All internal timeouts are
// derived from it (CommandTimeout for statements, ConnectionTimeout for
// begin/acquire/connect paths).
func (c *conn) base() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// timeout bounds ctx with the given command timeout.
func timeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

// freshRollbackCtx derives a context for a COMPENSATING rollback straight from
// context.Background(), bounded by CommandTimeout. Used where the primary
// statement just failed — a failed commit very often failed exactly because
// its context hit the deadline — so reusing that context would make the
// rollback instantly futile.
func freshRollbackCtx(o Options) (context.Context, context.CancelFunc) {
	return timeout(context.Background(), o.CommandTimeout)
}

// rollbackCtx prefers the caller's construction-time context (keeping its
// values/deadline lineage) and falls back to a fresh Background-backed
// context when that lineage is already done (canceled or expired). Used for
// fn-error rollbacks, where the original work failed for its own reasons and
// a live caller context is still meaningful.
func rollbackCtx(base context.Context, o Options) (context.Context, context.CancelFunc) {
	if base.Err() != nil {
		return freshRollbackCtx(o)
	}
	return timeout(base, o.CommandTimeout)
}

// track reports slow queries against the SlowQuery diagnostic threshold.
// Mantido para retrocompatibilidade do slog de warning; os hooks recebem TODA
// operação de qualquer forma (inclusive as lentas).
func track(o Options, start time.Time, sql string) {
	if o.SlowQuery > 0 && time.Since(start) > o.SlowQuery {
		slog.Warn("database: slow query", "duration", time.Since(start), "sql", sql)
	}
}

// ── Generic execution core ─────────────────────────────────────────
//
// Go methods cannot declare type parameters, so the typed equivalents of
// .NET IDatabaseExecutor.QueryAsync<T> / QueryFirstOrDefaultAsync<T> /
// ExecuteScalarAsync<T> are package-level functions parameterized by a conn
// (pool or transaction) instead of a receiver.

// runQuery maps every row into T using pgx conventions: exported fields
// matched by name or by a `db:"column"` tag. The context is the one captured
// once at construction, bounded by CommandTimeout.
//
// Hooks: BeforeHook dispara ANTES do derive do contexto; AfterHook dispara
// sempre (erro da query, erro de mapping, sucesso), com Rows=len(out) em caso
// de sucesso e rowsUnknown caso contrário. Com retry habilitado cada TENTATIVA
// dispara o par before/after (documentado no contrato de QueryHook).
func runQuery[T any](c *conn, sql string, args ...any) ([]T, error) {
	c.fireBefore(OpQuery, sql, args)

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.r.Query(cctx, sql, args...)
	if err != nil {
		track(c.o, start, sql)
		c.fireAfter(OpQuery, sql, args, time.Since(start), err, rowsUnknown)
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
	track(c.o, start, sql)

	if err != nil {
		c.fireAfter(OpQuery, sql, args, time.Since(start), err, rowsUnknown)
		return out, err
	}
	c.fireAfter(OpQuery, sql, args, time.Since(start), nil, int64(len(out)))
	return out, nil
}

// runQueryRow runs a query expected to return at most one row. found reports
// whether a row exists; an empty result is not an error (mirrors .NET's
// QueryFirstOrDefaultAsync returning null). The context is the one captured
// once at construction, bounded by CommandTimeout.
//
// Hooks: resultado vazio NÃO é erro para os hooks (semântica idêntica à
// biblioteca): AfterHook chega com Err=nil e Rows=0. Encontrado: Rows=1.
func runQueryRow[T any](c *conn, sql string, args ...any) (T, bool, error) {
	var zero T

	c.fireBefore(OpQueryRow, sql, args)

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	rows, err := c.r.Query(cctx, sql, args...)
	if err != nil {
		track(c.o, start, sql)
		c.fireAfter(OpQueryRow, sql, args, time.Since(start), err, rowsUnknown)
		return zero, false, err
	}
	value, err := pgx.CollectOneRow(rows, pgx.RowToStructByNameLax[T])
	track(c.o, start, sql)
	duration := time.Since(start)

	switch {
	case errors.Is(err, pgx.ErrNoRows):
		c.fireAfter(OpQueryRow, sql, args, duration, nil, 0)
		return zero, false, nil
	case err != nil:
		c.fireAfter(OpQueryRow, sql, args, duration, err, rowsUnknown)
		return zero, false, err
	default:
		c.fireAfter(OpQueryRow, sql, args, duration, nil, 1)
		return value, true, nil
	}
}

// runScalar scans a single-column, single-row result into T. The context is
// the one captured once at construction, bounded by CommandTimeout.
//
// Hooks: Rows permanece rowsUnknown — a existência/contagem de linhas não é
// fato observável pelo contrato scalar.
func runScalar[T any](c *conn, sql string, args ...any) (T, error) {
	c.fireBefore(OpScalar, sql, args)

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	var value T

	start := time.Now()
	err := c.r.QueryRow(cctx, sql, args...).Scan(&value)
	track(c.o, start, sql)
	c.fireAfter(OpScalar, sql, args, time.Since(start), err, rowsUnknown)

	return value, err
}

// Execute runs a command and returns affected rows. Shared through embedding:
// DB shadows it with a retry-wrapping version, Tx inherits it verbatim. The
// context is the one captured once at construction, bounded by CommandTimeout.
//
// Hooks: Rows recebe tag.RowsAffected() em sucesso e rowsUnknown em erro
// (incluindo erros de contexto, que ainda assim sempre disparam o AfterHook).
func (c *conn) Execute(sql string, args ...any) (int64, error) {
	c.fireBefore(OpExec, sql, args)

	cctx, cancel := timeout(c.base(), c.o.CommandTimeout)
	defer cancel()

	start := time.Now()
	tag, err := c.r.Exec(cctx, sql, args...)
	track(c.o, start, sql)
	if err != nil {
		c.fireAfter(OpExec, sql, args, time.Since(start), err, rowsUnknown)
		return 0, err
	}
	n := tag.RowsAffected()
	c.fireAfter(OpExec, sql, args, time.Since(start), nil, n)
	return n, nil
}

// retried applies p.do around fn, unifying the value-carrying retry boilerplate
// for every typed operation. The stored base context bounds the backoff sleep:
// cancellation during it surfaces as the joined ctx error (per-attempt contexts
// are derived internally from the same stored base context).
func retried[T any](ctx context.Context, p RetryPolicy, fn func() (T, error)) (T, error) {
	var out T
	err := p.do(ctx, func() error {
		var err error
		out, err = fn()
		return err
	})
	return out, err
}

// ── Public typed surface (DB) ─────────────────────────────────────

// Execute runs a command (INSERT/UPDATE/DELETE/DDL) and returns the number of
// affected rows. Transient failures are retried when retry is enabled. Every
// retry attempt fires QueryHooks again (one before/after pair per attempt).
func (db *DB) Execute(sql string, args ...any) (int64, error) {
	return retried(db.base(), db.retry, func() (int64, error) {
		return db.conn.Execute(sql, args...)
	})
}

// Query runs a SELECT and maps every row into a T. Transient failures are
// retried when retry is enabled. Every retry attempt fires QueryHooks again.
func Query[T any](db *DB, sql string, args ...any) ([]T, error) {
	return retried(db.base(), db.retry, func() ([]T, error) {
		return runQuery[T](&db.conn, sql, args...)
	})
}

// QueryRow runs a query expected to return at most one row. Transient failures
// are retried when retry is enabled. Every retry attempt fires QueryHooks again.
func QueryRow[T any](db *DB, sql string, args ...any) (T, bool, error) {
	var out T
	var found bool

	err := db.retry.do(db.base(), func() error {
		v, f, err := runQueryRow[T](&db.conn, sql, args...)
		out, found = v, f
		return err
	})

	return out, found, err
}

// Scalar runs a single-value query and scans it into T. Transient failures are
// retried when retry is enabled. Every retry attempt fires QueryHooks again.
func Scalar[T any](db *DB, sql string, args ...any) (T, error) {
	return retried(db.base(), db.retry, func() (T, error) {
		return runScalar[T](&db.conn, sql, args...)
	})
}
