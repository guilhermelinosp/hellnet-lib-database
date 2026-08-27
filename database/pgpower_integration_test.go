//go:build integration

package database

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// ── COPY / BulkInsert ───────────────────────────────────────────────

type bulkStatusRow struct {
	ID     int64  `db:"id"`
	Status string `db:"status"`
}

func TestIntegrationBulkInsertThousandRows(t *testing.T) {
	db := openIntegrationDB(t)

	if _, err := db.Execute(
		"CREATE TABLE bulk_target (id BIGINT PRIMARY KEY, status TEXT NOT NULL)"); err != nil {
		t.Fatalf("create bulk_target: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Execute("DROP TABLE IF EXISTS bulk_target") })

	conn, err := db.Acquire()
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() { _ = conn.Close() }()

	const n = 1000
	rows := make([][]any, 0, n)
	for i := range int64(n) {
		rows = append(rows, []any{i + 1, fmt.Sprintf("status-%d", i+1)})
	}

	copied, err := conn.BulkInsert("bulk_target", []string{"id", "status"}, rows)
	if err != nil || copied != n {
		t.Fatalf("BulkInsert = %d, %v; want %d, nil", copied, err, n)
	}

	count, err := Scalar[int64](db, "SELECT COUNT(*) FROM bulk_target")
	if err != nil || count != n {
		t.Fatalf("count = %d, %v; want %d", count, err, n)
	}

	// Spot-check one row landed intact.
	got, found, err := QueryRow[bulkStatusRow](db,
		"SELECT id, status FROM bulk_target WHERE id = $1", int64(500))
	if err != nil || !found || got.Status != "status-500" {
		t.Fatalf("spot-check row = %+v found=%v err=%v", got, found, err)
	}
}

// ── Batch pipelining inside a transaction ───────────────────────────

func TestIntegrationBatchInsertsAndCount(t *testing.T) {
	db := openIntegrationDB(t)
	resetOrdersTable(t, db)

	err := db.Transactional(func(tx *Tx) error {
		b := tx.Batch()
		b.Queue("INSERT INTO orders (status) VALUES ($1)", "b1")
		b.Queue("INSERT INTO orders (status) VALUES ($1)", "b2")
		b.Queue("INSERT INTO orders (status) VALUES ($1)", "b3")
		b.Queue("SELECT COUNT(*) FROM orders")

		br := tx.SendBatch(b)
		defer func() { _ = br.Close() }()

		for i := 1; i <= 3; i++ {
			n, err := br.ExecResult()
			if err != nil || n != 1 {
				return fmt.Errorf("ExecResult#%d = %d, %v; want 1, nil", i, n, err)
			}
		}
		var count int64
		if err := br.RowScan(&count); err != nil {
			return fmt.Errorf("RowScan count: %w", err)
		}
		if count != 3 {
			return fmt.Errorf("in-batch count = %d, want 3", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transactional(batch): %v", err)
	}

	persisted, err := Scalar[int64](db, "SELECT COUNT(*) FROM orders")
	if err != nil || persisted != 3 {
		t.Fatalf("post-commit count = %d, %v; want 3", persisted, err)
	}
}

// ── LISTEN/NOTIFY across two dedicated connections ──────────────────

func uniqueChannel(prefix string) string {
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), runtime.NumGoroutine())
}

func TestIntegrationListenNotifyTwoConns(t *testing.T) {
	opts := integrationOptions(t)
	ctx := context.Background()

	a, err := Connect(ctx, opts) // listener
	if err != nil {
		t.Fatalf("Connect(A): %v", err)
	}
	defer func() { _ = a.Close() }()

	b, err := Connect(ctx, opts) // notifier
	if err != nil {
		t.Fatalf("Connect(B): %v", err)
	}
	defer func() { _ = b.Close() }()

	channel := uniqueChannel("hn_evt")
	payloads := make(chan string, 16)
	stop, err := a.Listen(channel, func(p string) { payloads <- p })
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	// Give the LISTEN round trip room to land before B starts publishing.
	time.Sleep(150 * time.Millisecond)

	want := []string{"evt-1", "evt-2", "evt-3", "evt-4"}
	for _, p := range want {
		if err := b.Notify(channel, p); err != nil {
			t.Fatalf("Notify(%q): %v", p, err)
		}
	}

	received := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(received) < 3 {
		select {
		case p := <-payloads:
			received[p] = true
		case <-deadline:
			t.Fatalf("timeout; received only %v", received)
		}
	}
	for _, w := range want[:3] {
		if !received[w] {
			t.Errorf("payload %q not delivered (got %v)", w, received)
		}
	}

	start := time.Now()
	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stop took %s; must end cleanly ≤2s", elapsed)
	}

	// Drain any in-flight handlers already dispatched BEFORE stop (handlers
	// run on their own goroutines; joining them is not part of the stop
	// contract), then prove NEW notifications no longer arrive on this
	// session: the compensating UNLISTEN must be server-effective.
	drainDone := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case <-payloads:
		case <-drainDone:
			break drain
		}
	}

	if err := b.Notify(channel, "late"); err != nil {
		t.Fatalf("post-stop Notify: %v", err)
	}
	select {
	case p := <-payloads:
		t.Fatalf("delivered %q after stop()+UNLISTEN", p)
	case <-time.After(600 * time.Millisecond):
	}
}

func TestIntegrationListenWithReconnectReceives(t *testing.T) {
	opts := integrationOptions(t)
	ctx := context.Background()

	a, err := Connect(ctx, opts)
	if err != nil {
		t.Fatalf("Connect(A): %v", err)
	}
	defer func() { _ = a.Close() }()
	b, err := Connect(ctx, opts)
	if err != nil {
		t.Fatalf("Connect(B): %v", err)
	}
	defer func() { _ = b.Close() }()

	channel := uniqueChannel("hn_rec")
	payloads := make(chan string, 8)
	stop, err := a.ListenWithReconnect(channel, func(p string) { payloads <- p }, ListenOptions{})
	if err != nil {
		t.Fatalf("ListenWithReconnect: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if err := b.Notify(channel, "r1"); err != nil {
		t.Fatalf("Notify: %v", err)
	}
	select {
	case p := <-payloads:
		if p != "r1" {
			t.Fatalf("payload = %q, want r1", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no payload under reconnect policy")
	}

	if err := stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

// ── Streaming iteration with constant memory ────────────────────────

type iterNum struct {
	ID int64 `db:"id"`
}

func TestIntegrationIterateConstantMemoryAndSum(t *testing.T) {
	db := openIntegrationDB(t)

	const total = 5000
	if _, err := db.Execute(
		"CREATE TABLE iter_target (id INT PRIMARY KEY, pad TEXT NOT NULL DEFAULT '')"); err != nil {
		t.Fatalf("create iter_target: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Execute("DROP TABLE IF EXISTS iter_target") })

	if _, err := db.Execute(
		"INSERT INTO iter_target (id, pad) SELECT g, repeat('x', 64) FROM generate_series(1, $1) g", total); err != nil {
		t.Fatalf("seed %d rows: %v", total, err)
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	var sum int64
	err := Iter[iterNum](db, "SELECT id FROM iter_target ORDER BY id", nil, func(r iterNum) error {
		sum += r.ID
		return nil
	})
	if err != nil {
		t.Fatalf("Iter: %v", err)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	wantSum := int64(total) * int64(total+1) / 2 // 1..5000 == 12,502,500
	if sum != wantSum {
		t.Fatalf("sum = %d, want %d", sum, wantSum)
	}

	heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if heapGrowth < 0 {
		heapGrowth = 0 // GC between snapshots can shrink below start
	}
	const limit = 15 * 1024 * 1024
	if heapGrowth > limit {
		t.Fatalf("heap grew %.1fMB iterating %d rows; want < 15MB (constant memory violated)",
			float64(heapGrowth)/(1024*1024), total)
	}
	t.Logf("iterating %d rows: heap growth %.1fKB (limit 15MB)",
		total, float64(heapGrowth)/1024)
}

// Fn-error passthrough chain, end-to-end against real PG.
func TestIntegrationIterateAbortOnFnError(t *testing.T) {
	db := openIntegrationDB(t)

	abortErr := errors.New("consumer saw enough")
	calls := 0
	err := Iter[iterNum](db, "SELECT g AS id FROM generate_series(1, 100) g", nil, func(r iterNum) error {
		calls++
		if calls >= 7 {
			return abortErr
		}
		return nil
	})
	if !errors.Is(err, abortErr) {
		t.Fatalf("err = %v; want FnErr passthrough chain", err)
	}
	if calls != 7 {
		t.Fatalf("fn called %d times, want 7 (aborted at the threshold)", calls)
	}
}
