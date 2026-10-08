package database

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ── Fakes ───────────────────────────────────────────────────────────

// fakeListenRunner implements the basic runner plus notificationWaiter. Exec
// records every statement (optionally failing LISTEN attempts after the
// first); WaitForNotification pops preloaded notifications or fails with
// waitErr until the queue is empty.
type fakeListenRunner struct {
	fakePoolGate

	mu         sync.Mutex
	sqls       []string
	failListen bool // fail LISTEN statements after the initial one succeeded
	notifs     []*pgconn.Notification
	waitErr    error // returned when no notification is queued
	waits      int
	base       context.Context
}

func (f *fakeListenRunner) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	isListen := strings.HasPrefix(sql, "LISTEN")
	// Record EVERY statement — including attempts about to fail — so tests
	// can count retries.
	if isListen || strings.HasPrefix(sql, "UNLISTEN") {
		f.sqls = append(f.sqls, sql)
	}
	if isListen && f.failListen && len(f.sqls) > 1 {
		return pgconn.CommandTag{}, errors.New("fakeListenRunner: listen link down")
	}
	f.base = ctx
	return pgconn.CommandTag{}, nil
}

func (f *fakeListenRunner) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waits++
	if len(f.notifs) > 0 {
		n := f.notifs[0]
		f.notifs = f.notifs[1:]
		return n, nil
	}
	return nil, f.waitErr
}

func (f *fakeListenRunner) recordedSQL() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sqls...)
}

// ── Validation & mapping (deterministic unit scope) ─────────────────

func TestValidateChannel(t *testing.T) {
	valid := []string{"orders", "hn_events_2026", "A1_b2", "_leading"}
	for _, ch := range valid {
		if err := validateChannel(ch); err != nil {
			t.Errorf("validateChannel(%q) unexpected err: %v", ch, err)
		}
	}
	invalid := []string{"", "a b", "chan-el", "canal.português", "drop;table", `quo"te`}
	for _, ch := range invalid {
		if err := validateChannel(ch); err == nil {
			t.Errorf("validateChannel(%q) expected error", ch)
		} else if !strings.Contains(err.Error(), "invalid channel name") {
			t.Errorf("validateChannel(%q) err = %v, want prefixed descriptive message", ch, err)
		}
	}
}

func TestListenRejectsBadArgsBeforeTouchingBackend(t *testing.T) {
	c := newTestConn(&fakePoolGate{}) // would also fail capability check

	if _, err := c.Listen("bad channel", func(string) {}); err == nil ||
		!strings.Contains(err.Error(), "invalid channel name") {
		t.Fatalf("err = %v, want channel validation error", err)
	}
	if _, err := c.Listen("good_channel", nil); err == nil ||
		!strings.Contains(err.Error(), "handler must not be nil") {
		t.Fatalf("err = %v, want nil-handler error", err)
	}
}

func TestNotifyRejectsInvalidChannel(t *testing.T) {
	c := newTestConn(&fakePoolGate{})
	if err := c.Notify("spaced channel", "x"); err == nil ||
		!strings.Contains(err.Error(), "invalid channel name") {
		t.Fatalf("Notify err = %v, want validation error", err)
	}
}

func TestListenCapabilityMissing(t *testing.T) {
	// fakePoolGate: neither notificationWaiter nor rawConnProvider.
	c := newTestConn(&fakePoolGate{})
	_, err := c.Listen("events", func(string) {})
	if err == nil || !strings.Contains(err.Error(), "does not support LISTEN") {
		t.Fatalf("err = %v, want descriptive capability-missing error", err)
	}
}

func TestListenDeadStoredContext(t *testing.T) {
	fr := &fakeListenRunner{}
	base, cancel := context.WithCancel(context.Background())
	cancel()

	c := newTestConn(fr)
	c.ctx = base

	_, err := c.Listen("events", func(string) {})
	if err == nil || !strings.Contains(err.Error(), "already done") {
		t.Fatalf("err = %v, want dead-lineage error before any LISTEN is sent", err)
	}
	if got := fr.recordedSQL(); len(got) != 0 {
		t.Errorf("LISTEN was executed (%v) despite a dead stored context", got)
	}
}

// ── Happy path: LISTEN sent, payload dispatched, stop joins + UNLISTEN ──

func TestListenDispatchAndCleanStop(t *testing.T) {
	fr := &fakeListenRunner{
		notifs: []*pgconn.Notification{
			{Channel: "events", Payload: "p1"},
			{Channel: "events", Payload: "p2"},
			{Channel: "events", Payload: "p3"},
		},
	}

	c := newTestConn(fr)

	payloads := make(chan string, 8)
	stop, err := c.Listen("events", func(p string) { payloads <- p })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Initial LISTEN ran synchronously.
	got := fr.recordedSQL()
	if len(got) < 1 || got[0] != "LISTEN events" {
		t.Fatalf("initial SQL = %v, want [LISTEN events]", got)
	}

	// Handlers run on their own goroutines — delivery ORDER is not part of
	// the contract, only that every payload arrives.
	received := map[string]bool{}
	deadline := time.After(3 * time.Second)
	for len(received) < 3 {
		select {
		case p := <-payloads:
			received[p] = true
		case <-deadline:
			t.Fatalf("timeout; received only %v", received)
		}
	}
	for _, want := range []string{"p1", "p2", "p3"} {
		if !received[want] {
			t.Errorf("payload %q not delivered (got %v)", want, received)
		}
	}

	start := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; must end within ~1 tick (≤2s)", elapsed)
	}

	got = fr.recordedSQL()
	if len(got) < 2 || got[len(got)-1] != "UNLISTEN events" {
		t.Fatalf("SQL trail = %v, want trailing UNLISTEN events", got)
	}

	// Second stop must be safe (idempotent via sync.Once + joined goroutine).
	if err := stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

// providerShapeRunner mirrors *pgxpool.Conn's shape (runner + .Conn()
// accessor); compile-time coverage of the REAL types lives in listen.go's var
// block. It proves the second capability-resolution branch is taken instead of
// failing with the descriptive error.
type providerShapeRunner struct {
	fakePoolGate
}

func (providerShapeRunner) Conn() *pgx.Conn { return nil } // shape only — never invoked here

func TestListenResolvesThroughRawConnProvider(t *testing.T) {
	if _, err := resolveListenBackend(&providerShapeRunner{}); err != nil {
		t.Fatalf("provider-shaped runner rejected: %v", err)
	}
	if _, err := resolveListenBackend(&fakePoolGate{}); err == nil ||
		!strings.Contains(err.Error(), "does not support LISTEN") {
		t.Fatalf("plain runner err = %v, want capability-missing", err)
	}
}

// ── Reconnect policy: re-listen loop + Warn logging ─────────────────

func TestListenWithReconnectRetriesUntilStop(t *testing.T) {
	harness := telemetry.NewHarness(t)

	// First LISTEN succeeds (initial setup), then wait always fails with a
	// transport error and every further LISTEN attempt fails → the policy must
	// keep retrying until stop, logging each attempt.
	fr := &fakeListenRunner{
		failListen: true,
		waitErr:    errors.New("boom: broken pipe"),
	}

	c := newTestConn(fr)
	c.o.CommandTimeout = 250 * time.Millisecond
	c.obs = newObservability(harness)

	payloads := make(chan string, 4)
	stop, err := c.ListenWithReconnect("events", func(p string) { payloads <- p },
		ListenOptions{RetryDelay: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("ListenWithReconnect: %v", err)
	}

	listens := func() int {
		n := 0
		for _, sql := range fr.recordedSQL() {
			if strings.HasPrefix(sql, "LISTEN") {
				n++
			}
		}
		return n
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && listens() < 4 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := listens(); n < 4 {
		t.Fatalf("only %d re-listen attempts within 2s; reconnect policy stalled", n)
	}
	if len(harness.Logs()) == 0 {
		t.Error("no contract log recorded for re-listen attempts")
	}

	// Stop joins promptly and issues its compensating UNLISTEN (this fake
	// keeps Exec alive; only LISTEN attempts fail).
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	trail := fr.recordedSQL()
	if len(trail) < 5 {
		t.Fatalf("SQL trail = %v; want multiple LISTEN attempts plus UNLISTEN", trail)
	}
	if last := trail[len(trail)-1]; last != "UNLISTEN events" {
		t.Errorf("final statement = %q, want UNLISTEN events", last)
	}
}

// Give-up policy (plain Listen): first transport failure ends the loop.
func TestListenGivesUpOnTransportError(t *testing.T) {
	fr := &fakeListenRunner{waitErr: errors.New("boom: connection reset")}
	c := newTestConn(fr)
	c.o.CommandTimeout = 250 * time.Millisecond

	stop, err := c.Listen("events", func(string) {})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// The loop gives up on its own after the first transport error; stop must
	// still join cleanly (UNLISTEN issues fine because Exec succeeds here).
	start := time.Now()
	if sErr := stop(); sErr != nil {
		t.Fatalf("stop after give-up: %v", sErr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; want prompt return", elapsed)
	}

	trail := fr.recordedSQL()
	if len(trail) != 2 || trail[0] != "LISTEN events" || trail[1] != "UNLISTEN events" {
		t.Fatalf("SQL trail = %v, want [LISTEN events UNLISTEN events]", trail)
	}
}

type noTraceClient struct{ t *testing.T }

func (c noTraceClient) Trace(context.Context) telemetry.ContextTracer {
	c.t.Helper()
	c.t.Fatal("LISTEN must not open a span through the telemetry client")
	return telemetry.ContextTracer{}
}

func TestListenOpensNoDBExecuteSpan(t *testing.T) {
	fr := &fakeListenRunner{}
	c := newTestConn(fr)
	c.conn.ops = noTraceClient{t}

	stop, err := c.Listen("events", func(string) {})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = stop() }()
	if got := fr.recordedSQL(); len(got) < 1 || got[0] != "LISTEN events" {
		t.Fatalf("initial SQL = %v, want [LISTEN events]", got)
	}
}

// A dead pinned connection (every re-LISTEN fails) must be replaced by a fresh
// pooled one: notifications keep flowing and stop() releases the replacement.
func TestListenWithReconnectReacquiresFreshConnection(t *testing.T) {
	dead := &fakeListenRunner{failListen: true, waitErr: errors.New("conn closed")}
	fresh := &fakeListenRunner{waitErr: context.DeadlineExceeded,
		notifs: []*pgconn.Notification{{Channel: "events", Payload: "after-reacquire"}}}

	c := newTestConn(dead)
	c.o.CommandTimeout = 250 * time.Millisecond
	c.o.ConnectionTimeout = 250 * time.Millisecond
	var mu sync.Mutex
	released, acquired := 0, 0
	c.reacquireFn = func(context.Context) (runner, func(context.Context) error, error) {
		mu.Lock()
		defer mu.Unlock()
		acquired++
		return fresh, func(context.Context) error { mu.Lock(); released++; mu.Unlock(); return nil }, nil
	}

	payloads := make(chan string, 4)
	stop, err := c.ListenWithReconnect("events", func(p string) { payloads <- p },
		ListenOptions{RetryDelay: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, ReacquireAfter: 2})
	if err != nil {
		t.Fatalf("ListenWithReconnect: %v", err)
	}
	select {
	case got := <-payloads:
		if got != "after-reacquire" {
			t.Fatalf("payload = %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no notification delivered after the dead connection was replaced")
	}
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if acquired != 1 || released != 1 {
		t.Errorf("acquired=%d released=%d, want 1/1 (replacement must be released on stop)", acquired, released)
	}
	if trail := fresh.recordedSQL(); len(trail) < 2 || trail[0] != "LISTEN events" || trail[len(trail)-1] != "UNLISTEN events" {
		t.Errorf("fresh connection SQL trail = %v, want LISTEN ... UNLISTEN", trail)
	}
}

// A standalone Conn (no reacquire function) keeps retrying on its own session.
func TestListenWithReconnectWithoutReacquireKeepsRetrying(t *testing.T) {
	dead := &fakeListenRunner{failListen: true, waitErr: errors.New("conn closed")}
	c := newTestConn(dead)
	c.o.CommandTimeout = 250 * time.Millisecond
	stop, err := c.ListenWithReconnect("events", func(string) {},
		ListenOptions{RetryDelay: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond, ReacquireAfter: 1})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if n := len(dead.recordedSQL()); n < 4 {
		t.Errorf("only %d statements recorded; retries stalled", n)
	}
}
