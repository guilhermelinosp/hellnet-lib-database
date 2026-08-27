package database

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	io_prometheus "github.com/prometheus/client_model/go"
)

// gatherFamilies junta as families de um registro e indexa por nome.
func gatherFamilies(t *testing.T, reg *prometheus.Registry) map[string]*io_prometheus.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*io_prometheus.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func TestStatusLabelDerivation(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"sucesso":       {nil, metricStatusOK},
		"erro comum":    {errors.New("boom"), metricStatusError},
		"canceled":      {context.Canceled, metricStatusCanceled},
		"deadline":      {context.DeadlineExceeded, metricStatusCanceled},
		"canceled join": {errors.Join(errors.New("x"), context.Canceled), metricStatusCanceled},
	}
	for name, tc := range cases {
		if got := statusLabel(tc.err); got != tc.want {
			t.Errorf("%s: statusLabel = %q, want %q", name, got, tc.want)
		}
	}
}

// O collector responde ao contrato QueryHook direto (sem precisar de runner):
// counters movem por op/status e o histograma expõe os buckets explícitos.
func TestMetricsCollectorAfterHookCounts(t *testing.T) {
	reg := prometheus.NewRegistry()
	mc := NewMetricsCollector(reg)
	mc.pollInterval = time.Hour // sem sampler aqui; dispatch manual
	if err := mc.register(); err != nil {
		t.Fatalf("register: %v", err)
	}

	mc.AfterHook(QueryInfo{Op: OpExec, Duration: 120 * time.Millisecond})
	mc.AfterHook(QueryInfo{Op: OpExec, Duration: 80 * time.Millisecond})
	mc.AfterHook(QueryInfo{Op: OpQuery, Duration: 600 * time.Millisecond})
	mc.AfterHook(QueryInfo{Op: OpQuery, Err: context.Canceled, Duration: 5 * time.Millisecond})
	mc.BeforeHook(QueryInfo{Op: OpExec}) // no-op documentado

	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpExec, metricStatusOK)); got != 2 {
		t.Errorf("db_queries_total{exec,ok} = %v, want 2", got)
	}
	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpQuery, metricStatusOK)); got != 1 {
		t.Errorf("db_queries_total{query,ok} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpQuery, metricStatusCanceled)); got != 1 {
		t.Errorf("db_queries_total{query,canceled} = %v, want 1", got)
	}

	mfs := gatherFamilies(t, reg)
	hist, ok := mfs["db_query_duration_seconds"]
	if !ok || hist.GetType() != io_prometheus.MetricType_HISTOGRAM {
		t.Fatalf("histograma ausente/tipo errado: %#v", hist)
	}
	var saw25, saw5 bool
	for _, b := range hist.GetMetric()[0].GetHistogram().GetBucket() {
		switch b.GetUpperBound() {
		case 0.25:
			saw25 = true
			if b.GetCumulativeCount() != 2 { // observações de 120ms E 80ms caem em le=0.25
				t.Errorf("le=0.25 count = %d, want 2", b.GetCumulativeCount())
			}
		case 5:
			saw5 = true
		}
	}
	if !saw25 || !saw5 {
		t.Error("buckets explícitos esperados (0.25 e 5) não encontrados — distribuição lenta (5ms..10s) quebrada")
	}

	// Sinais fora do hook.
	mc.recordTx(txResultCommit)
	mc.recordTx(txResultRollback)
	mc.recordRollbackCompensation()
	mc.observeAcquire()
	if got := testutil.ToFloat64(mc.txTotal.WithLabelValues(txResultCommit)); got != 1 {
		t.Errorf("tx{commit} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(mc.rollbackCompensations); got != 1 {
		t.Errorf("rollback_compensations = %v, want 1", got)
	}
	if got := testutil.ToFloat64(mc.poolAcquireTotal); got != 1 {
		t.Errorf("pool_acquires = %v, want 1", got)
	}
}

// Ponta-a-ponta sobre fakes da própria lib: EnableMetrics anexa o collector,
// contadores movem após queries reais do fluxo e os gauges nascem coerentes
// com Stat() (amostra inicial síncrona).
func TestEnableMetricsEndToEndOnFakePool(t *testing.T) {
	pool := &fakeRunnerPool{
		execRows: 3,
		stats:    PoolStats{InUse: 7, Idle: 3, Max: 100},
	}
	db := newHookTestDB(t, pool, Options{})

	reg := prometheus.NewRegistry()
	handle, err := db.EnableMetrics(reg)
	if err != nil {
		t.Fatalf("EnableMetrics: %v", err)
	}
	defer handle.Close()

	if _, err := db.Execute("UPDATE orders SET status = 'x'"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := Query[hookRow](db, "SELECT id, status FROM orders"); err != nil {
		t.Fatalf("Query: %v", err)
	}
	// Tx feliz registra commit via registry compartilhado (fake tx no-op).
	tx := &fakeTx{}
	gate := &fakePoolGate{tx: tx, stats: pool.stats}
	dbTx := &DB{
		conn:  db.conn,
		pool:  gate,
		retry: NewRetryPolicy(false, 0, time.Millisecond),
	}
	if err := dbTx.Transactional(func(*Tx) error { return nil }); err != nil {
		t.Fatalf("Transactional: %v", err)
	}

	mc := handle.Collector()
	if mc == nil {
		t.Fatal("handle.Collector() nil")
	}
	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpExec, metricStatusOK)); got < 1 {
		t.Errorf("db_queries_total{exec,ok} = %v, want >=1", got)
	}
	if got := testutil.ToFloat64(mc.queriesTotal.WithLabelValues(OpQuery, metricStatusOK)); got < 1 {
		t.Errorf("db_queries_total{query,ok} = %v, want >=1", got)
	}

	// Gauges refletem o fake Stat imediatamente (amostra síncrona).
	if got := testutil.ToFloat64(mc.poolInUse); got != 7 {
		t.Errorf("db_pool_in_use = %v, want 7", got)
	}
	if got := testutil.ToFloat64(mc.poolIdle); got != 3 {
		t.Errorf("db_pool_idle = %v, want 3", got)
	}
	if got := testutil.ToFloat64(mc.poolMax); got != 100 {
		t.Errorf("db_pool_max = %v, want 100", got)
	}

	mfs := gatherFamilies(t, reg)
	for _, name := range []string{
		"db_queries_total", "db_query_duration_seconds", "db_pool_acquires_total",
		"db_transactions_total", "db_rollback_compensations_total",
		"db_pool_in_use", "db_pool_idle", "db_pool_max",
	} {
		if _, ok := mfs[name]; !ok {
			t.Errorf("familia %s não registrada", name)
		}
	}
	if got := testutil.ToFloat64(mc.txTotal.WithLabelValues(txResultCommit)); got != 1 {
		t.Errorf("db_transactions_total{commit} = %v, want 1 (registry compartilhado com Tx)", got)
	}
}

func TestEnableMetricsDoubleEnableAndHandleCloseIdempotent(t *testing.T) {
	pool := &fakeRunnerPool{stats: PoolStats{InUse: 1, Idle: 1, Max: 10}}
	db := newHookTestDB(t, pool, Options{})

	reg := prometheus.NewRegistry()
	handle, err := db.EnableMetrics(reg)
	if err != nil {
		t.Fatalf("primeiro EnableMetrics: %v", err)
	}

	if _, err := db.EnableMetrics(reg); err == nil {
		t.Error("segundo EnableMetrics deveria falhar (already enabled)")
	}

	handle.Close()
	handle.Close() // idempotente

	mfs := gatherFamilies(t, reg)
	if len(mfs) != 0 {
		t.Errorf("após Close ainda há %d famílias registradas (%v), want 0", len(mfs), keysOf(mfs))
	}

	// Re-habilitação permitida após Close (documentado).
	again, err := db.EnableMetrics(reg)
	if err != nil {
		t.Fatalf("re-EnableMetrics pós-Close: %v", err)
	}
	defer again.Close()
	if got := testutil.ToFloat64(again.Collector().poolMax); got != 10 {
		t.Errorf("gauges pós-re-enable = %v, want 10", got)
	}
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
