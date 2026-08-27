package database

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ── Read-replica routing ────────────────────────────────────────────
//
// Cluster fronts one writable primary plus N read replicas behind a single
// handle. Writes ALWAYS go to the primary; reads rotate across replicas with
// an atomic round-robin counter and fall back to the primary when no replica
// is configured — so a one-line NewCluster(primary) already gives callers a
// future-proof routing point.
//
// Design invariants:
//   - ctx-once compliant: like every type here, no public method takes a
//     context.Context; each member DB keeps its own construction-time
//     context and derived timeouts.
//   - The cluster dials NOTHING. Callers build each DB separately via
//     New/OpenFromEnv/ConnectFromEnv (replicas typically inherit the SAME
//     Options overwritten only on Host/Port/credentials as needed), then
//     hand them over. Topology is captured by value at construction:
//     adding or removing replicas later means building a new Cluster.
//   - Reads delegate to the replica's own retry policy; there is no cluster-
//     level failover in v1 — a broken replica surfaces its error directly.

// ErrNilPrimary is returned by NewCluster when the primary is missing.
var ErrNilPrimary = errors.New("database: cluster requires a non-nil primary DB")

// Cluster routes writes to primary and reads across replicas.
type Cluster struct {
	primary  *DB
	replicas []*DB
	rr       atomic.Uint32
}

// NewCluster assembles a read-routing cluster around the mandatory primary;
// replicas may be empty. Every pointer is validated up front so later nil
// dereferences become impossible by construction (fail-fast beats panic).
func NewCluster(primary *DB, replicas ...*DB) (*Cluster, error) {
	if primary == nil {
		return nil, ErrNilPrimary
	}
	for i, r := range replicas {
		if r == nil {
			return nil, fmt.Errorf("database: cluster replica at index %d is nil", i)
		}
	}
	return &Cluster{primary: primary, replicas: append([]*DB(nil), replicas...)}, nil
}

// Primary returns the write-owning DB.
func (cl *Cluster) Primary() *DB { return cl.primary }

// Replica returns the next read target: round-robin across configured
// replicas, or the primary itself when none exist. Safe under concurrency —
// the rotation rides an atomic counter, and multiple in-flight readers may
// observe overlapping indices without tearing anything.
func (cl *Cluster) Replica() *DB {
	n := len(cl.replicas)
	if n == 0 {
		return cl.primary
	}
	// Widening uint32→int cannot lose data on any Go platform (int is ≥32
	// bits), keeping gosec G115's lossy-conversion concern out of the path.
	next := int(cl.rr.Add(1) - 1)
	return cl.replicas[next%n]
}

// Execute routes to PRIMARY and returns affected rows (passthrough).
func (cl *Cluster) Execute(sql string, args ...any) (int64, error) {
	return cl.Primary().Execute(sql, args...)
}

// Transactional routes to PRIMARY (passthrough): transactions always own the
// writable node, regardless of isolation level.
func (cl *Cluster) Transactional(fn func(tx *Tx) error) error {
	return cl.Primary().Transactional(fn)
}

// TransactionalLevel routes to PRIMARY (passthrough), validating the level
// before any begin. See (*DB).TransactionalLevel for the full contract.
func (cl *Cluster) TransactionalLevel(level string, fn func(tx *Tx) error) error {
	return cl.Primary().TransactionalLevel(level, fn)
}

// ── Read passthroughs ───────────────────────────────────────────────
//
// Package-level rather than methods because Go methods cannot carry type
// parameters — the same constraint that produced Query[T]/TxQuery[T].
// They mirror the core signatures minus ctx, rotating across Replica() per
// call.

// ClusterQuery runs a SELECT against the next round-robin REPLICA (or the
// primary when none are configured) mapping rows into T.
func ClusterQuery[T any](cl *Cluster, sql string, args ...any) ([]T, error) {
	if cl == nil {
		return nil, errors.New("database: ClusterQuery requires a non-nil Cluster")
	}
	return Query[T](cl.Replica(), sql, args...)
}

// ClusterQueryRow runs at-most-one-row reads on the next REPLICA; empty
// results yield (zero, false, nil), matching QueryRow[T].
func ClusterQueryRow[T any](cl *Cluster, sql string, args ...any) (T, bool, error) {
	var zero T
	if cl == nil {
		return zero, false, errors.New("database: ClusterQueryRow requires a non-nil Cluster")
	}
	return QueryRow[T](cl.Replica(), sql, args...)
}

// ClusterScalar scans a single value from the next REPLICA.
func ClusterScalar[T any](cl *Cluster, sql string, args ...any) (T, error) {
	if cl == nil {
		var zero T
		return zero, errors.New("database: ClusterScalar requires a non-nil Cluster")
	}
	return Scalar[T](cl.Replica(), sql, args...)
}
