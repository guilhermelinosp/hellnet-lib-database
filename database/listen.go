package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// notifyTick is how often the fetch loop wakes up without notifications so a
// stop request is always honored within ~1 tick.
const notifyTick = time.Second

// channelRe validates LISTEN/NOTIFY channel names: plain identifiers only
// ([a-zA-Z0-9_]+). Because PostgreSQL does not accept bind parameters in the
// LISTEN/UNLISTEN statements, the name is interpolated into the SQL — this
// regex is what makes that safe.
var channelRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// validateChannel rejects channel names outside the safe charset.
func validateChannel(channel string) error {
	if !channelRe.MatchString(channel) {
		return fmt.Errorf("database: invalid channel name %q (allowed: [a-zA-Z0-9_]+)", channel)
	}
	return nil
}

// notificationWaiter is the native async-notification support of *pgx.Conn.
// The wait deadline comes from ctx (so each fetch iteration bounds itself by
// notifyTick).
type notificationWaiter interface {
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
}

// rawConnProvider is implemented by pool connections (*pgxpool.Conn): the
// pooled handle forwards to an inner *pgx.Conn which actually waits.
type rawConnProvider interface {
	Conn() *pgx.Conn
}

// ListenOptions tunes the re-listen policy used by ListenWithReconnect.
type ListenOptions struct {
	// RetryDelay is the first backoff delay after a failed re-listen attempt.
	// <= 0 means 500ms.
	RetryDelay time.Duration
	// MaxBackoff caps the exponential growth between attempts. <= 0 means 5s.
	MaxBackoff time.Duration
}

// normalized fills zero values with the documented defaults.
func (o ListenOptions) normalized() ListenOptions {
	if o.RetryDelay <= 0 {
		o.RetryDelay = 500 * time.Millisecond
	}
	if o.MaxBackoff < o.RetryDelay {
		o.MaxBackoff = 5 * time.Second
		if o.MaxBackoff < o.RetryDelay {
			o.MaxBackoff = o.RetryDelay
		}
	}
	return o
}

// resolveListenBackend finds something able to WaitForNotification on this
// connection: the runner itself (*pgx.Conn) or its inner native connection
// (*pgxpool.Conn.Conn()). Exec always stays on the runner surface.
func resolveListenBackend(r runner) (notificationWaiter, error) {
	if w, ok := r.(notificationWaiter); ok {
		return w, nil
	}
	if p, ok := r.(rawConnProvider); ok {
		return p.Conn(), nil
	}
	return nil, fmt.Errorf("database: this connection does not support LISTEN")
}

// Listen registers handler for async notifications published on channel and
// returns immediately after the initial LISTEN succeeds. On the first fatal
// connection error the fetch loop gives up (the Warn is logged); use
// ListenWithReconnect when the delivery must survive dropped connections.
//
// The returned stop releases everything: it ends the fetch goroutine (~1s at
// most, one notifyTick) and issues a best-effort UNLISTEN on the session.
// Handler invocations run on their own goroutines so a slow handler never
// blocks deliveries; note stop() does NOT wait for an in-flight handler.
//
// The context captured once at New/Connect is used internally; each fetch
// iteration is bounded by a 1s tick instead of CommandTimeout, since waiting
// for events is expected to outlive any command deadline.
func (c *Conn) Listen(channel string, handler func(payload string)) (stop func() error, err error) {
	return c.listen(channel, handler, ListenOptions{}, false)
}

// ListenWithReconnect is Listen with a re-listen policy: when the fetch loop
// hits a connection error it re-executes `LISTEN channel` forever, with
// exponential backoff from opts.RetryDelay up to opts.MaxBackoff, logging each
// attempt at WARN level, until stop() is called. Note this re-listens the SAME
// pinned connection — if the physical link died permanently the attempts keep
// failing until the caller stops and rebuilds the Conn; it covers transient
// interruptions where pgx can keep using the session.
func (c *Conn) ListenWithReconnect(channel string, handler func(payload string), opts ListenOptions) (stop func() error, err error) {
	return c.listen(channel, handler, opts.normalized(), true)
}

// listen is the shared body of Listen and ListenWithReconnect. The initial
// LISTEN runs synchronously so setup errors surface at call time.
func (c *Conn) listen(channel string, handler func(payload string), opts ListenOptions, reconnect bool) (func() error, error) {
	if err := validateChannel(channel); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, fmt.Errorf("database: listen handler must not be nil")
	}
	waiter, err := resolveListenBackend(c.r)
	if err != nil {
		return nil, err
	}
	if base := c.base(); base.Err() != nil {
		return nil, fmt.Errorf("database: listen %q: stored context is already done: %w", channel, base.Err())
	}

	// Initial LISTEN interpolated with a strictly validated identifier — no
	// bind parameters exist for LISTEN at the protocol level.
	if _, err := c.Execute(listenSQL(channel)); err != nil {
		return nil, fmt.Errorf("database: listen %q: %w", channel, err)
	}

	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	go listenLoop(&c.conn, waiter, channel, handler, opts, reconnect, stopCh, doneCh)

	var stopOnce sync.Once
	stopFn := func() error {
		stopOnce.Do(func() { close(stopCh) })
		<-doneCh // join the fetch goroutine: clean shutdown within ~1 tick

		// Compensating UNLISTEN prefers the live stored lineage and falls back
		// to a fresh Background-backed timeout when that lineage is done.
		uctx, ucancel := rollbackCtx(c.base(), c.o)
		defer ucancel()
		if _, uerr := c.r.Exec(uctx, unlistenSQL(channel)); uerr != nil {
			return fmt.Errorf("database: unlisten %q: %w", channel, uerr)
		}
		return nil
	}
	return stopFn, nil
}

// listenSQL builds `LISTEN channel` (channel pre-validated).
func listenSQL(channel string) string { return fmt.Sprintf("LISTEN %s", channel) }

// unlistenSQL builds `UNLISTEN channel` (channel pre-validated).
func unlistenSQL(channel string) string { return fmt.Sprintf("UNLISTEN %s", channel) }

// sleepCancellable waits d or until stopCh closes; reports whether d elapsed.
func sleepCancellable(d time.Duration, stopCh <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-stopCh:
		return false
	}
}

// listenLoop is the dedicated fetch goroutine: wait (1s tick) → dispatch the
// handler on ANOTHER goroutine (never block the fetch loop) → repeat. On real
// failures it either gives up (v1 policy) or re-listens with backoff until
// stopped.
func listenLoop(
	c *conn,
	waiter notificationWaiter,
	channel string,
	handler func(payload string),
	opts ListenOptions,
	reconnect bool,
	stopCh <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)

	backoff := opts.RetryDelay

	for {
		// A dead stored lineage cannot produce new valid tick contexts.
		if base := c.base(); base.Err() != nil {
			slog.Warn("database: listen stopped: stored context is done", "channel", channel)
			return
		}
		select {
		case <-stopCh:
			return
		default:
		}

		wctx, wcancel := timeout(c.base(), notifyTick)
		n, err := waiter.WaitForNotification(wctx)
		wcancel()

		switch {
		case err == nil:
			if n != nil {
				go handler(n.Payload)
			}
			backoff = opts.RetryDelay // healthy again: reset any grown backoff
			continue
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			continue // routine empty tick (or caller teardown: caught top-of-loop)
		}

		// Real transport failure below.
		select {
		case <-stopCh:
			return
		default:
		}
		if !reconnect {
			slog.Warn("database: listen aborted by connection error",
				"channel", channel, "error", err)
			return
		}
		slog.Warn("database: listen failed; scheduling re-listen",
			"channel", channel, "error", err, "retry_in", backoff)

		for {
			if !sleepCancellable(backoff, stopCh) {
				return
			}
			backoff = min(backoff*2, opts.MaxBackoff)

			if _, lerr := c.Execute(listenSQL(channel)); lerr == nil {
				slog.Warn("database: re-listen succeeded", "channel", channel)
				break
			} else {
				slog.Warn("database: re-listen attempt failed",
					"channel", channel, "error", lerr, "next_retry_in", backoff)
			}
		}
	}
}

// Notify publishes payload on channel through `SELECT pg_notify($1,$2)` (bind
// parameters ARE supported here). The channel is validated like Listen.
// PostgreSQL limits payloads to 8000 bytes — larger ones fail server-side.
// The context captured once at New/Connect is used internally, bounded by
// CommandTimeout.
func (c *Conn) Notify(channel string, payload string) error {
	if err := validateChannel(channel); err != nil {
		return err
	}
	if _, err := c.Execute("SELECT pg_notify($1,$2)", channel, payload); err != nil {
		return fmt.Errorf("database: notify %q: %w", channel, err)
	}
	return nil
}

// Compile-time capability wiring: standalone *pgx.Conn waits natively; pool
// connections reach the native conn through the .Conn() accessor.
var (
	_ notificationWaiter = (*pgx.Conn)(nil)
	_ rawConnProvider    = (*pgxpool.Conn)(nil)
	_ runner             = (*pgx.Conn)(nil)
	_ runner             = (*pgxpool.Conn)(nil)
)
