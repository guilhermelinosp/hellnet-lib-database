package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// nonTransientSQLStates lists PostgreSQL SQLSTATE codes that must NEVER be
// retried: they are permanent errors where retrying cannot succeed and would
// only mask a bug or a data problem. Mirrors the .NET DatabaseRetryPolicy.
var nonTransientSQLStates = map[string]string{
	"42601": "syntax_error",
	"23505": "unique_violation",
	"23503": "foreign_key_violation",
	"42501": "insufficient_privilege",
	"42P01": "undefined_table",
	"42703": "undefined_column",
}

// RetryPolicy retries transient failures with exponential backoff. Permanent
// PostgreSQL errors (syntax, constraint violations, privileges, missing
// schema objects) are returned immediately; so is context cancellation.
type RetryPolicy struct {
	enabled   bool
	maxCount  int
	baseDelay time.Duration
}

// NewRetryPolicy builds a retry policy. When enabled is false Do executes fn
// exactly once.
func NewRetryPolicy(enabled bool, maxCount int, baseDelay time.Duration) RetryPolicy {
	return RetryPolicy{
		enabled:   enabled,
		maxCount:  maxCount,
		baseDelay: baseDelay,
	}
}

// IsTransient reports whether err describes a failure worth retrying:
// any error except context cancellation/deadline and the permanent
// PostgreSQL SQLSTATEs listed in nonTransientSQLStates.
func (p RetryPolicy) IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if _, permanent := nonTransientSQLStates[pgErr.Code]; permanent {
			return false
		}
	}
	return true
}

// shouldRetry reports whether attempt (0-based) may be followed by another one.
func (p RetryPolicy) shouldRetry(err error, attempt int) bool {
	if !p.enabled || err == nil || attempt >= p.maxCount {
		return false
	}
	return p.IsTransient(err)
}

// sleep waits for the exponential backoff delay after attempt (0-based):
// baseDelay << attempt. It observes ctx cancellation so a canceled caller does
// not wait out the full backoff (pattern shared with hellnet-lib-kafka's
// dlq.go), returning ctx.Err() when the wait is interrupted.
func (p RetryPolicy) sleep(ctx context.Context, attempt int) error {
	t := time.NewTimer(p.baseDelay << attempt)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do runs fn retrying transient failures up to maxCount times with exponential
// backoff. A disabled policy runs fn once. The backoff sleep is bounded by
// context.Background() — callers holding the library-captured base context go
// through do instead.
func (p RetryPolicy) Do(fn func() error) error {
	return p.do(context.Background(), fn)
}

// do is Do with the library-owned base context: backoff sleeps are interruptible,
// and cancellation during them returns the last operation error joined with
// ctx.Err() so neither failure mode is silently swallowed.
func (p RetryPolicy) do(ctx context.Context, fn func() error) error {
	if !p.enabled {
		return fn()
	}

	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !p.shouldRetry(err, attempt) {
			return err
		}
		slog.Warn("database: transient error, retrying",
			"attempt", attempt+2, "max", p.maxCount+1, "error", err)
		if sleepErr := p.sleep(ctx, attempt); sleepErr != nil {
			return errors.Join(err, fmt.Errorf("database: retry aborted: %w", sleepErr))
		}
	}
}
