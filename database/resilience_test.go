package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code string) *pgconn.PgError {
	return &pgconn.PgError{Code: code, Message: "pg error " + code}
}

func TestIsTransientPermanentStates(t *testing.T) {
	p := NewRetryPolicy(true, 3, time.Millisecond)

	for code, name := range nonTransientSQLStates {
		if p.IsTransient(pgErr(code)) {
			t.Errorf("SQLSTATE %s (%s) classified as transient, want permanent", code, name)
		}
	}
}

func TestIsTransient(t *testing.T) {
	p := NewRetryPolicy(true, 3, time.Millisecond)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic error", errors.New("boom"), true},
		{"transient pg error", pgErr("53300"), true}, // too_many_connections
		{"context canceled", context.Canceled, false},
		{"deadline exceeded", context.DeadlineExceeded, false},
		{"wrapped canceled", errors.Join(errors.New("x"), context.Canceled), false},
	}

	for _, tc := range tests {
		if got := p.IsTransient(tc.err); got != tc.want {
			t.Errorf("%s: IsTransient = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDoDisabledRunsOnce(t *testing.T) {
	p := NewRetryPolicy(false, 5, time.Millisecond)

	calls := 0
	err := p.Do(func() error {
		calls++
		return errors.New("permanent for this policy")
	})

	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
	if err == nil {
		t.Error("Do returned nil, want the fn error")
	}
}

func TestDoRetriesUntilSuccess(t *testing.T) {
	p := NewRetryPolicy(true, 5, time.Millisecond)

	calls := 0
	err := p.Do(func() error {
		calls++
		if calls < 3 {
			return pgErr("08006") // connection_failure — transient
		}
		return nil
	})

	if err != nil {
		t.Errorf("Do = %v, want nil", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDoGivesUpAfterMaxCount(t *testing.T) {
	p := NewRetryPolicy(true, 2, time.Millisecond)

	calls := 0
	_ = p.Do(func() error {
		calls++
		return pgErr("08006")
	})

	// 1 initial attempt + maxCount retries.
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDoesNotRetryPermanentErrors(t *testing.T) {
	p := NewRetryPolicy(true, 5, time.Millisecond)

	calls := 0
	err := p.Do(func() error {
		calls++
		return pgErr("23505") // unique_violation — permanent
	})

	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry for unique violations)", calls)
	}
	if err == nil {
		t.Error("err = nil, want the pg error")
	}
}

func TestDoesNotRetryCancellationErrors(t *testing.T) {
	p := NewRetryPolicy(true, 10, time.Millisecond)

	// With context propagated internally, cancellation surfaces as an error
	// returned by fn — and must never be retried.
	for _, cerr := range []error{context.Canceled, context.DeadlineExceeded} {
		calls := 0
		err := p.Do(func() error {
			calls++
			return cerr
		})
		if calls != 1 || !errors.Is(err, cerr) {
			t.Errorf("%v: calls=%d err=%v, want 1 call returning the ctx error", cerr, calls, err)
		}
	}
}

// TestDoAbortsBackoffWhenBaseContextCanceled pins the I2 contract: while the
// policy sleeps between attempts, base-context cancellation interrupts the
// backoff instead of waiting it out, and the result carries BOTH the last
// operation error and the ctx error (errors.Join).
func TestDoAbortsBackoffWhenBaseContextCanceled(t *testing.T) {
	p := NewRetryPolicy(true, 5, 200*time.Millisecond) // first backoff would cost 200ms

	opErr := pgErr("08006") // connection_failure — transient, keeps retrying
	cctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	calls := 0
	err := p.do(cctx, func() error {
		calls++
		return opErr
	})

	cancel()
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (cancellation must interrupt the first backoff)", calls)
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, opErr) {
		t.Errorf("err = %v, want joined op error + context.Canceled", err)
	}
}

func TestSleepInterruptedByContextCancellation(t *testing.T) {
	p := NewRetryPolicy(true, 2, 30*time.Second)

	cctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- p.sleep(cctx, 0) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("sleep err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sleep ignored an already-cancelled context")
	}
}

func TestSleepBackoff(t *testing.T) {
	p := NewRetryPolicy(true, 2, time.Millisecond)

	start := time.Now()
	if err := p.sleep(context.Background(), 2); err != nil { // baseDelay << 2 = 4ms
		t.Fatalf("sleep(2) err = %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed < 4*time.Millisecond {
		t.Errorf("sleep(2) returned after %s, want >= 4ms", elapsed)
	}
}

// TestDoValueCarriesValueAndRetries pins the DoValue contract: same exponential
// behavior as Do, but the fn may return a value — transient failures retry and
// the eventual value surfaces; permanent SQLSTATEs do not.
func TestDoValueCarriesValueAndRetries(t *testing.T) {
	p := NewRetryPolicy(true, 5, time.Millisecond)

	calls := 0
	got, err := DoValue(p, func() (string, error) {
		calls++
		if calls < 3 {
			return "", pgErr("08006") // connection_failure — transient
		}
		return "payload", nil
	})
	if err != nil || got != "payload" {
		t.Errorf("DoValue = (%q, %v), want (\"payload\", nil) after retries", got, err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}

	// Permanent error: exactly one call, zero value returned.
	calls = 0
	zero, err := DoValue(p, func() (int, error) {
		calls++
		return -1, pgErr("23505") // unique_violation — permanent
	})
	var asPg *pgconn.PgError
	if calls != 1 || !errors.As(err, &asPg) || asPg.Code != "23505" {
		t.Errorf("permanent path: calls=%d err=%v, want single call surfacing SQLSTATE 23505", calls, err)
	}
	if zero != 0 {
		t.Errorf("on permanent error the value should be T's zero, got %d", zero)
	}

	// Disabled policy: runs once and delivers the value unchanged.
	disabled := NewRetryPolicy(false, 9, time.Millisecond)
	value, err := DoValue(disabled, func() (int, error) { return 42, nil })
	if err != nil || value != 42 {
		t.Errorf("disabled policy: (%d, %v), want (42, nil)", value, err)
	}
}
