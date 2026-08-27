package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ── Métricas Prometheus nativas (sem OTel!) ─────────────────────────────
//
// Este pacote NÃO importa nada de OpenTelemetry: o collector abaixo é
// Prometheus puro e ao mesmo tempo um QueryHook — recebe todas as operações
// pelos mesmos pontos de interceptação públicos da lib.
//
// Família de métricas (prefixo db_):
//
//	db_queries_total{op,status}              counter  ok|error|canceled
//	db_query_duration_seconds{op}            histograma buckets explícitos 5ms..10s
//	db_pool_acquires_total                   counter  (DB.Acquire bem-sucedidos)
//	db_transactions_total{result}            counter  commit|rollback|panic
//	db_rollback_compensations_total          counter  rollbacks compensatórios
//	db_pool_in_use / db_pool_idle / db_pool_max   gauges amostrados por goroutine

// Valores derivados do label status (dos erros de cada operação).
const (
	metricStatusOK       = "ok"
	metricStatusError    = "error"
	metricStatusCanceled = "canceled"
)

// Valores do label result em db_transactions_total.
const (
	txResultCommit   = "commit"
	txResultRollback = "rollback"
	txResultPanic    = "panic"
)

// queryDurationBuckets cobre explicitamente a distribuição de lentidão das
// queries (alinhada à percepção do threshold SlowQuery): de 5ms até 10s.
var queryDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// defaultMetricsPollInterval é o intervalo de amostragem dos gauges de pool.
const defaultMetricsPollInterval = 5 * time.Second

// statusLabel deriva o valor do label status a partir do erro observado:
// nil→ok; contexto cancelado/deadline→canceled; qualquer outro→error.
func statusLabel(err error) string {
	switch {
	case err == nil:
		return metricStatusOK
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return metricStatusCanceled
	default:
		return metricStatusError
	}
}

// MetricsCollector é um QueryHook embutido que traduz o fluxo de operações em
// métricas Prometheus nativas. Crie com NewMetricsCollector e habilite via
// DB.EnableMetrics (fluxo recomendado), ou registre os collectors manualmente
// via Collect(). A instância implementa AMBOS os hooks.
type MetricsCollector struct {
	reg prometheus.Registerer

	queriesTotal          *prometheus.CounterVec   // op,status
	queryDuration         *prometheus.HistogramVec // op
	poolAcquireTotal      prometheus.Counter
	txTotal               *prometheus.CounterVec // result
	rollbackCompensations prometheus.Counter

	poolInUse prometheus.Gauge
	poolIdle  prometheus.Gauge
	poolMax   prometheus.Gauge

	pollInterval time.Duration // intervalo do sampler; default 5s (ajustável em testes)
}

// Compile-time proof: o collector é um QueryHook completo (before + after).
var _ QueryHook = (*MetricsCollector)(nil)

// NewMetricsCollector monta o collector pronto para uso. reg nil assume
// prometheus.DefaultRegisterer como alvo de registro/unregistro; passar um
// registro próprio (prometheus.NewRegistry) isola as métricas do global.
// A construção NÃO registra nada — o registro acontece em DB.EnableMetrics
// (ou manualmente via Collect()).
func NewMetricsCollector(reg prometheus.Registerer) *MetricsCollector {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	mc := &MetricsCollector{
		reg:          reg,
		pollInterval: defaultMetricsPollInterval,
	}

	mc.queriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "db_queries_total",
		Help: "database statements executed by op (query|query_row|scalar|exec) and status (ok|error|canceled)",
	}, []string{"op", "status"})

	mc.queryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "db_query_duration_seconds",
		Help:    "statement execution latency (inclui mapping) por op",
		Buckets: queryDurationBuckets,
	}, []string{"op"})

	mc.poolAcquireTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "db_pool_acquires_total",
		Help: "successful dedicated connections acquired from the pool (DB.Acquire)",
	})

	mc.txTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "db_transactions_total",
		Help: "transaction outcomes by result (commit|rollback|panic)",
	}, []string{"result"})

	mc.rollbackCompensations = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "db_rollback_compensations_total",
		Help: "compensating rollbacks executed outside the normal path (post-commit-failure, deferred-after-panic)",
	})

	mc.poolInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_pool_in_use",
		Help: "pool connections currently checked out (sampled)",
	})
	mc.poolIdle = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_pool_idle",
		Help: "idle pool connections awaiting reuse (sampled)",
	})
	mc.poolMax = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "db_pool_max",
		Help: "configured pool connection limit (sampled)",
	})

	return mc
}

// Collect devolve todos os collectors gerenciados para registro/gerência
// externa (útil quando o chamador prefere registrar por conta própria).
func (mc *MetricsCollector) Collect() []prometheus.Collector {
	return []prometheus.Collector{
		mc.queriesTotal,
		mc.queryDuration,
		mc.poolAcquireTotal,
		mc.txTotal,
		mc.rollbackCompensations,
		mc.poolInUse,
		mc.poolIdle,
		mc.poolMax,
	}
}

// register registra todos os collectors em mc.reg; se algum falhar, desfaz
// os parciais para não vazar estado pela metade.
func (mc *MetricsCollector) register() error {
	registered := make([]prometheus.Collector, 0, len(mc.Collect()))
	for _, c := range mc.Collect() {
		if err := mc.reg.Register(c); err != nil {
			for _, done := range registered {
				_ = mc.reg.Unregister(done)
			}
			return fmt.Errorf("database: registering metrics: %w", err)
		}
		registered = append(registered, c)
	}
	return nil
}

// unregister remove todos os collectors de mc.reg (uso interno do handle).
func (mc *MetricsCollector) unregister() {
	for _, c := range mc.Collect() {
		_ = mc.reg.Unregister(c)
	}
}

// ── QueryHook ────────────────────────────────────────────────────────────

// BeforeHook é propositalmente no-op: a duração chega pronta no AfterHook
// (QueryInfo.Duration), medida pela própria lib na volta da execução.
func (mc *MetricsCollector) BeforeHook(QueryInfo) {}

// AfterHook materializa a observação: incrementa db_queries_total{op,status}
// e observa a duração em db_query_duration_seconds{op}. O status é derivado
// do erro (ok | canceled p/ contexto morto | error para todo o resto).
func (mc *MetricsCollector) AfterHook(info QueryInfo) {
	if mc == nil {
		return
	}
	status := statusLabel(info.Err)
	mc.queriesTotal.WithLabelValues(info.Op, status).Inc()
	mc.queryDuration.WithLabelValues(info.Op).Observe(info.Duration.Seconds())
}

// ── Sinais fora do contrato QueryHook (transações/acquire/pool) ──────────

// recordTx registra o desfecho estrutural de uma transação
// (txResultCommit | txResultRollback | txResultPanic). Nil-safe.
func (mc *MetricsCollector) recordTx(result string) {
	if mc == nil {
		return
	}
	mc.txTotal.WithLabelValues(result).Inc()
}

// recordRollbackCompensation conta rollbacks compensatórios — os que rodam
// FORA do caminho feliz (falha de commit, pânico em fn). Nil-safe.
func (mc *MetricsCollector) recordRollbackCompensation() {
	if mc == nil {
		return
	}
	mc.rollbackCompensations.Inc()
}

// observeAcquire incrementa db_pool_acquires_total após um Acquire bem-
// sucedido. Nil-safe.
func (mc *MetricsCollector) observeAcquire() {
	if mc == nil {
		return
	}
	mc.poolAcquireTotal.Inc()
}

// samplePool atualiza os três gauges a partir de um snapshot do pool. Também
// invocada sincronamente no EnableMetrics para os gauges nascerem coerentes.
func (mc *MetricsCollector) samplePool(s PoolStats) {
	mc.poolInUse.Set(float64(s.InUse))
	mc.poolIdle.Set(float64(s.Idle))
	mc.poolMax.Set(float64(s.Max))
}

// pollLoop amostra periodicamente o pool até o cancelamento do handle.
func (mc *MetricsCollector) pollLoop(ctx context.Context, pool Pool) {
	ticker := time.NewTicker(mc.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.samplePool(pool.Stat()) // pgxpool.Stat é seguro após Close
		}
	}
}

// ── Ciclo de vida dentro do DB ───────────────────────────────────────────

// MetricsHandle controla o ciclo de vida das métricas habilitadas por
// EnableMetrics: Close interrompe o sampler do pool, desliga o hook e desfaz
// o registro no Registerer informado. É idempotente (sync.Once) — chamar mais
// de uma vez não tem efeito. Depois de Close o DB continua funcionando
// normalmente; também é possível habilitar métricas novamente.
type MetricsHandle struct {
	once   sync.Once
	mc     *MetricsCollector
	stop   func()
	detach func()
}

// Collector devolve o collector subjacente (leitura avançada de métricas em
// testes ou exposition customizado).
func (h *MetricsHandle) Collector() *MetricsCollector {
	if h == nil {
		return nil
	}
	return h.mc
}

// Close para o sampler, desanexa o hook e desfaz o registro das métricas.
// Idempotente e nil-safe.
func (h *MetricsHandle) Close() {
	if h == nil {
		return
	}
	h.once.Do(func() {
		h.stop()
		h.detach()
		h.mc.unregister()
	})
}

// EnableMetrics registra o MetricsCollector nativo (Prometheus puro, sem
// OTel) como QueryHook deste DB — valendo para todas as transações/conexões
// criadas depois — inicia o sampler dos gauges de pool e devolve um handle
// cujo Close() interrompe tudo de forma idempotente.
//
// Regras:
//   - Segunda chamada num mesmo DB retorna erro ("already enabled"); após
//     handle.Close() é permitido habilitar de novo.
//   - reg nil usa prometheus.DefaultRegisterer (evite: prefira registro
//     isolado para testes/exposição controlada).
//   - O chamador é dono do handle: feche-o antes do app encerrar para não
//     deixar goroutine nem métricas órfãs no registro.
func (db *DB) EnableMetrics(reg prometheus.Registerer) (*MetricsHandle, error) {
	if db.hooks.observed() != nil {
		return nil, errors.New("database: metrics already enabled for this DB")
	}

	target := reg
	if target == nil {
		target = prometheus.DefaultRegisterer
	}
	mc := NewMetricsCollector(target)
	if err := mc.register(); err != nil {
		return nil, err
	}

	db.hooks.add(mc)
	db.hooks.setObserved(mc)

	// Amostra inicial síncrona: os gauges ficam coerentes já no retorno;
	// a goroutine seguinte só mantém o refresh periódico.
	mc.samplePool(db.pool.Stat())
	ctx, cancel := context.WithCancel(context.Background())
	go mc.pollLoop(ctx, db.pool)

	return &MetricsHandle{
		mc:   mc,
		stop: cancel,
		detach: func() {
			db.hooks.remove(mc)
			db.hooks.setObserved(nil)
		},
	}, nil
}
