package database

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ── clamp policy ────────────────────────────────────────────────────

func TestWithCommandTimeoutClamps(t *testing.T) {
	cases := []struct {
		name      string
		requested time.Duration
		effective time.Duration
	}{
		{"negative clamped", -time.Second, minCommandTimeout},
		{"zero clamped", 0, minCommandTimeout},
		{"sub-floor clamped", 10 * time.Millisecond, minCommandTimeout},
		{"exact floor honored", minCommandTimeout, minCommandTimeout},
		{"custom honored", 80 * time.Millisecond, 80 * time.Millisecond},
		{"large honored", time.Hour, time.Hour},
	}
	for _, tc := range cases {
		db := newTestDB(context.Background(), newRecordingPool())
		v := db.WithCommandTimeout(tc.requested)
		if v == nil {
			t.Fatalf("%s: nil view", tc.name)
		}
		if got := v.CommandTimeout(); got != tc.effective {
			t.Errorf("%s: CommandTimeout() = %v, want %v", tc.name, got, tc.effective)
		}
	}
}

// The parent's knobs must remain untouched — views are shallow overrides.
func TestViewImmutability(t *testing.T) {
	db := newTestDB(context.WithValue(context.Background(), ctxKeyMarker{}, "lineage"), newRecordingPool())
	base := db.Options().CommandTimeout

	fast := db.WithCommandTimeout(80 * time.Millisecond)
	slow := db.WithCommandTimeout(90 * time.Millisecond)

	if fast.CommandTimeout() != 80*time.Millisecond || slow.CommandTimeout() != 90*time.Millisecond {
		t.Fatalf("views crossed wires: %v / %v", fast.CommandTimeout(), slow.CommandTimeout())
	}
	if after := db.Options().CommandTimeout; after != base || base != 2*time.Second {
		t.Errorf("parent Options mutated by view creation: %v (want %v)", after, base)
	}

	if v := (*DBView)(nil).CommandTimeout(); v != 0 {
		t.Errorf("nil view CommandTimeout = %v, want 0", v)
	}
}

// ── deadline propagation through the statement core ─────────────────

// remainingUntil converts a recorded absolute deadline into its duration at
// call time is not possible post-hoc; instead assert the window disjoint from
// the base timeout so an override falling back to defaults fails loudly.
func assertDeadlineNear(t *testing.T, rec queryRecord, want time.Duration) {
	t.Helper()
	if !rec.HasDeadline {
		t.Fatal("statement context carried no deadline")
	}
	got := rec.CtxDeadline
	now := time.Now()
	if !got.After(now.Add(-want)) && !got.After(now) {
		// already expired — accept only when close to the request window
		t.Fatalf("deadline %v already passed (requested %v)", got, want)
	}
	until := time.Until(got)
	low := want - 60*time.Millisecond
	high := want + 60*time.Millisecond
	if until < low || until > high {
		t.Errorf("remaining until deadline = %v, want within [%v, %v]", until, low, high)
	}
}

func TestViewExecuteUsesCustomTimeoutAndLineage(t *testing.T) {
	pool := newRecordingPool()
	pool.setCommandTag("UPDATE 2")
	db := newTestDB(context.WithValue(context.Background(), ctxKeyMarker{}, "lineage"), pool)

	n, err := db.WithCommandTimeout(250*time.Millisecond).Execute("UPDATE t SET x=1", 9)
	if err != nil || n != 2 {
		t.Fatalf("view Execute = %d, %v; want 2,nil", n, err)
	}

	recs := pool.execCalls()
	if len(recs) != 1 || recs[0].SQL != "UPDATE t SET x=1" {
		t.Fatalf("execs = %+v", recs)
	}
	assertDeadlineNear(t, recs[0], 250*time.Millisecond)
	if recs[0].Lineage != "lineage" {
		t.Errorf("derived ctx lost construction-time lineage (%v)", recs[0].Lineage)
	}
	assertArgs(t, recs[0].Args, []any{9})
}

func TestViewQueryRowDeadlineTight(t *testing.T) {
	pool := newRecordingPool()
	pool.enqueueRows([]string{"id", "status"}, []any{int64(3), "fast"})
	db := newTestDB(context.Background(), pool)

	row, found, err := ViewQueryRow[repoRow](db.WithCommandTimeout(100*time.Millisecond),
		`SELECT "id", "status" FROM t WHERE id=$1`, 3)
	if err != nil || !found {
		t.Fatalf("ViewQueryRow found=%v err=%v; want mapped row", found, err)
	}
	if row.ID != 3 || row.Status != "fast" {
		t.Errorf("row = %+v", row)
	}

	rec := pool.queries()[0]
	assertDeadlineNear(t, rec, 100*time.Millisecond)

	// The tight custom timeout must be far from the parent default (2s).
	if time.Until(rec.CtxDeadline) > time.Second {
		t.Errorf("override ignored: deadline still ~parent default (%v)", time.Until(rec.CtxDeadline))
	}
}

func TestViewQueryEmptyResult(t *testing.T) {
	pool := newRecordingPool() // no queued rows → empty result set
	db := newTestDB(context.Background(), pool)

	rows, err := ViewQuery[repoRow](db.WithCommandTimeout(75*time.Millisecond), `SELECT "id" FROM t`)
	if err != nil || len(rows) != 0 {
		t.Errorf("ViewQuery empty = %v rows, %v; want 0,nil", len(rows), err)
	}
	if got := pool.queryCount(); got != 1 {
		t.Errorf("queries=%d, want 1", got)
	}
	assertDeadlineNear(t, pool.queries()[0], 75*time.Millisecond)
}

func TestViewScalarUnderCustomTimeout(t *testing.T) {
	pool := newRecordingPool()
	pool.enqueueScalar(int64(42))
	db := newTestDB(context.Background(), pool)

	v, err := ViewScalar[int64](db.WithCommandTimeout(120*time.Millisecond), "SELECT 42")
	if err != nil || v != 42 {
		t.Fatalf("ViewScalar = %v, %v", v, err)
	}
	assertDeadlineNear(t, pool.queries()[0], 120*time.Millisecond)
}

// ── misuse guards ───────────────────────────────────────────────────

func TestUninitializedViewRejected(t *testing.T) {
	var uninitialized *DBView

	if _, err := uninitialized.Execute("SELECT 1"); !errors.Is(err, ErrInvalidView) {
		t.Errorf("nil view Execute err=%v, want ErrInvalidView", err)
	}
	if _, _, err := ViewQueryRow[string](uninitialized, "q"); !errors.Is(err, ErrInvalidView) {
		t.Errorf("nil view ViewQueryRow err=%v, want ErrInvalidView", err)
	}
	if _, err := ViewQuery[string](uninitialized, "q"); !errors.Is(err, ErrInvalidView) {
		t.Errorf("nil view ViewQuery err=%v, want ErrInvalidView", err)
	}
	if _, err := ViewScalar[string](uninitialized, "q"); !errors.Is(err, ErrInvalidView) {
		t.Errorf("nil view ViewScalar err=%v, want ErrInvalidView", err)
	}
	if _, err := (&DBView{}).Execute("x"); !errors.Is(err, ErrInvalidView) {
		t.Errorf("zero-value view Execute err=%v, want ErrInvalidView", err)
	}
	if err := errors.Is(nilViewExecuteHelper("x"), ErrInvalidView); !err {
		t.Error("helper path broke ErrInvalidView contract")
	}
}

func nilViewExecuteHelper(sql string) error {
	_, err := (*DBView)(nil).Execute(sql)
	return err
}
