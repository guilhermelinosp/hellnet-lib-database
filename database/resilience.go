package database

import (
	"context"
	"errors"
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
// baseDelay << attempt. It returns early on context cancellation.
func (p RetryPolicy) sleep(ctx context.Context, attempt int) error {
	delay := p.baseDelay << attempt

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Do runs fn retrying transient failures up to maxCount times with exponential
// backoff. A disabled policy runs fn once.
func (p RetryPolicy) Do(ctx context.Context, fn func() error) error {
	if !p.enabled {
		return fn()
	}

	var err error
	for attempt := 0; ; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || !p.shouldRetry(err, attempt) {
			return err
		}
		slog.Warn("database: transient error, retrying",
			"attempt", attempt+2, "max", p.maxCount+1, "error", err)
		if serr := p.sleep(ctx, attempt); serr != nil {
			return err
		}
	}
}
