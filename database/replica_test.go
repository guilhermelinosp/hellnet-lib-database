package database

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
)

// replicaTag makes pool identity human-visible in failure messages.
func replicaLabel(t *testing.T, cl *Cluster, target *DB) string {
	t.Helper()
	switch {
	case target == cl.Primary():
		return "primary"
	default:
		return "replica"
	}
}

func TestNewClusterValidation(t *testing.T) {
	if _, err := NewCluster(nil); !errors.Is(err, ErrNilPrimary) {
		t.Errorf("NewCluster(nil) err=%v, want ErrNilPrimary", err)
	}
	if _, err := NewCluster(nil, newTestDB(context.Background(), newRecordingPool())); !errors.Is(err, ErrNilPrimary) {
		t.Errorf("NewCluster(nil-with-replica) err=%v, want ErrNilPrimary", err)
	}

	ok := newTestDB(context.Background(), newRecordingPool())
	_, err := NewCluster(ok, nil)
	if err == nil || !strings.Contains(err.Error(), "replica at index 0 is nil") {
		t.Errorf("nil replica index 0 err=%v, want named-index error", err)
	}
	a := newTestDB(context.Background(), newRecordingPool())
	b := newTestDB(context.Background(), newRecordingPool())
	if _, err := NewCluster(ok, a, nil, b); err == nil ||
		!strings.Contains(err.Error(), "replica at index 1 is nil") {
		t.Errorf("nil replica mid-slice err=%v, want index 1 named", err)
	}
}

func TestReplicaRoundRobinSequence(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	replicas := []*DB{
		newTestDB(context.Background(), newRecordingPool()),
		newTestDB(context.Background(), newRecordingPool()),
		newTestDB(context.Background(), newRecordingPool()),
	}
	cl, err := NewCluster(primary, replicas...)
	if err != nil {
		t.Fatalf("NewCluster err=%v", err)
	}

	for round := 0; round < 3; round++ {
		for i, expect := range replicas {
			got := cl.Replica()
			if got != expect {
				t.Fatalf("round %d pick %d = %s, want next replica in A,B,C rotation",
					round, i, replicaLabel(t, cl, got))
			}
		}
	}
}

func TestReplicaFallbackWhenEmpty(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	cl, err := NewCluster(primary)
	if err != nil {
		t.Fatalf("NewCluster empty-replicas err=%v", err)
	}
	for i := 0; i < 5; i++ {
		if got := cl.Replica(); got != primary {
			t.Fatalf("pick %d returned non-primary despite no replicas", i)
		}
	}
}

func TestClusterTopologyCapturedByValue(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	a := newTestDB(context.Background(), newRecordingPool())
	reps := []*DB{a}
	cl, _ := NewCluster(primary, reps...)

	// Mutating the caller's slice must not reshape the cluster.
	reps[0] = primary
	if got := cl.Replica(); got != a {
		t.Fatal("cluster aliases the caller's replica slice instead of a copy")
	}
	if len(cl.replicas) != 1 || cl.replicas[0] != a {
		t.Fatalf("internal topology = %+v, want the original single replica", cl.replicas)
	}
}

func TestClusterWritesAlwaysRouteToPrimary(t *testing.T) {
	primaryPool := newRecordingPool()
	primaryPool.setCommandTag("UPDATE 5")
	primary := newTestDB(context.Background(), primaryPool)

	r1 := newTestDB(context.Background(), newRecordingPool())
	r2 := newTestDB(context.Background(), newRecordingPool())
	cl, _ := NewCluster(primary, r1, r2)

	n, err := cl.Execute("UPDATE orders SET ok=true", 1)
	if err != nil || n != 5 {
		t.Fatalf("cluster Execute = %d, %v; want 5,nil (tag passthrough)", n, err)
	}

	err = cl.Transactional(func(tx *Tx) error { return nil })
	if err != nil {
		t.Fatalf("cluster Transactional err=%v", err)
	}

	err = cl.TransactionalLevel("serializable", func(tx *Tx) error { return nil })
	if err != nil {
		t.Fatalf("cluster TransactionalLevel err=%v", err)
	}

	if primaryPool.execCount() == 0 {
		t.Fatalf("primary must receive the Execute call; saw exec=%d", primaryPool.execCount())
	}
	opts := primaryPool.txOptionsSeen()
	if len(opts) != 1 || opts[0].IsoLevel != pgx.Serializable {
		t.Fatalf("level plumb-through on cluster path: %+v, want exactly one serializable begin", opts)
	}
	if got := len(primaryPool.begunTxs); got != 2 {
		t.Errorf("primary transactions begun = %d, want 2 (plain + leveled)", got)
	}
	for i, rp := range []*recordingPool{r1.pool.(*recordingPool), r2.pool.(*recordingPool)} {
		if rp.execCount() != 0 || rp.queryCount() != 0 || len(rp.begunTxs) != 0 {
			t.Errorf("replica %d received write traffic it must never see: %+v", i, rp.queries())
		}
	}
}

func TestClusterReadsRotateAcrossReplicas(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	pools := []*recordingPool{newRecordingPool(), newRecordingPool(), newRecordingPool()}
	var replicas []*DB
	for _, p := range pools {
		replicas = append(replicas, newTestDB(context.Background(), p))
	}
	cl, _ := NewCluster(primary, replicas...)

	const per = 2 // full sequence repeated per replica
	for range per {
		for _, p := range pools {
			if _, err := ClusterQuery[repoRow](cl, `SELECT "id" FROM t`); err != nil {
				t.Fatalf("ClusterQuery err=%v; want empty,nil", err)
			}
			_ = p // rotation, not pool identity, is what this loop asserts
		}
	}

	for i, p := range pools {
		if got := p.queryCount(); got != per {
			t.Errorf("replica %d served %d reads, want exactly %d (strict A,B,C,A… rotation)", i, got, per)
		}
	}
	if primary.pool.(*recordingPool).queryCount() != 0 {
		t.Error("reads leaked onto the primary")
	}
}

func TestClusterScalarAndQueryRowHitNextReplica(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	poolA := newRecordingPool()
	poolB := newRecordingPool()
	cl, _ := NewCluster(primary,
		newTestDB(context.Background(), poolA),
		newTestDB(context.Background(), poolB))

	poolA.enqueueScalar(true)
	ok, err := ClusterScalar[bool](cl, "SELECT true")
	if err != nil || !ok {
		t.Fatalf("ClusterScalar = %v,%v", ok, err)
	}

	poolB.enqueueRows([]string{"id"}, []any{int64(11)})
	row, found, err := ClusterQueryRow[repoRow](cl, "SELECT id FROM t")
	if err != nil || !found || row.ID != 11 {
		t.Fatalf("ClusterQueryRow = %+v,%v,%v", row, found, err)
	}

	if poolA.queryCount() != 1 || poolB.queryCount() != 1 {
		t.Errorf("scalar/row routing counts: A=%d B=%d, want one call each",
			poolA.queryCount(), poolB.queryCount())
	}
}

func TestClusterConcurrentReplicaRotation(t *testing.T) {
	primary := newTestDB(context.Background(), newRecordingPool())
	replicas := []*DB{
		newTestDB(context.Background(), newRecordingPool()),
		newTestDB(context.Background(), newRecordingPool()),
		newTestDB(context.Background(), newRecordingPool()),
	}
	cl, _ := NewCluster(primary, replicas...)

	counts := make([]int32, len(replicas))
	total := 800
	workers := 8
	perWorker := total / workers

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				got := cl.Replica()
				for j, r := range replicas {
					if got == r {
						atomic.AddInt32(&counts[j], 1)
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	sum := int32(0)
	for j, c := range counts {
		if c == 0 {
			t.Errorf("replica %d starved under concurrency", j)
		}
		sum += c
	}
	if sum != int32(total) {
		t.Errorf("rotation lost picks: %d of %d accounted for", sum, total)
	}
}

func TestClusterNilReceiversRejected(t *testing.T) {
	if _, err := ClusterQuery[string](nil, "q"); err == nil ||
		!strings.Contains(err.Error(), "non-nil Cluster") {
		t.Errorf("ClusterQuery(nil) err=%v", err)
	}
	if _, _, err := ClusterQueryRow[string](nil, "q"); err == nil ||
		!strings.Contains(err.Error(), "non-nil Cluster") {
		t.Errorf("ClusterQueryRow(nil) err=%v", err)
	}
	if _, err := ClusterScalar[string](nil, "q"); err == nil ||
		!strings.Contains(err.Error(), "non-nil Cluster") {
		t.Errorf("ClusterScalar(nil) err=%v", err)
	}
}
