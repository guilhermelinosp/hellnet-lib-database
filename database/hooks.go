package database

import (
	"sync"
	"time"
)

// ── QueryHook — interceptação de queries sem acoplamento a OTel ───────────
//
// O pacote expõe um contrato mínimo de observabilidade (QueryHook) no lugar
// de depender de OpenTelemetry/Prometheus. Qualquer stack (OTel bridge,
// Prometheus nativo, logging, auditoria) implementa duas funções e recebe TODA
// operação executada por DB/Conn/Tx.
//
// Contrato:
//   - Hooks NÃO devem entrar em pânico. A biblioteca recupera o pânico de um
//     hook individual (slog.Warn) para não quebrar queries nem os demais
//     hooks — defesa em profundidade, não licença.
//   - BeforeHook roda ANTES do derive do contexto de timeout; AfterHook é
//     always-called (sucesso, erro de statement, erro de contexto) e chega em
//     pares com o Before de cada TENTATIVA — retry habilitado dispara o par
//     uma vez por tentativa.
//   - Hooks devem ser rápidos e não bloquear: são chamados inline na hot path.

// Op values usados em QueryInfo.Op (e nos labels `op` das métricas).
const (
	OpQuery    = "query"     // Query[T] / TxQuery[T] / ConnQuery[T]
	OpQueryRow = "query_row" // QueryRow[T] / TxQueryRow[T] / ConnQueryRow[T]
	OpScalar   = "scalar"    // Scalar[T] / TxScalar[T] / ConnScalar[T]
	OpExec     = "exec"      // Execute
)

// rowsUnknown marca Rows quando a contagem não é observável (queries que
// falharam antes da leitura, scalar e erros de mapping).
const rowsUnknown int64 = -1

// QueryInfo descreve uma operação interceptada pelos hooks.
type QueryInfo struct {
	// Op classifica a operação: "query"|"query_row"|"scalar"|"exec".
	Op string
	// SQL é a sentença exatamente como executada.
	SQL string
	// Args são os args posicionais; nil quando escondidos
	// (Options.HideQueryArgs) ou quando a operação não tem args.
	Args []any
	// Duration é 0 no pre-hook; no after-hook cobre statement + mapping.
	Duration time.Duration
	// Err é nil em sucesso e no pre-hook. Resultado vazio de query_row NÃO é
	// erro (semântica da lib); erro de contexto é repassado tal como veio.
	Err error
	// Rows é -1 (desconhecido) para queries sem contagem observável;
	// len(rows) em query bem-sucedida, 0/1 em query_row, rowsAffected em exec.
	Rows int64
}

// QueryHook observa o ciclo de vida de cada operação. Implementações devem
// registrar-se via Options.QueryHooks (antes do New/Connect) ou receber o
// collector embutido via DB.EnableMetrics.
type QueryHook interface {
	// BeforeHook é chamado antes da execução (Duration zerado).
	BeforeHook(info QueryInfo)
	// AfterHook é sempre chamado após a execução — inclusive nos caminhos de
	// erro (statement OU contexto), em par com o Before da mesma tentativa.
	// Fases são independentes: pânico contido num Before de um hook não
	// impede os outros hooks nem o After da operação.
	AfterHook(info QueryInfo)
}

// QueryHookFunc adapta funções anônimas ao QueryHook (zero-value friendly:
// campos nil simplesmente não disparam).
type QueryHookFunc struct {
	Before func(QueryInfo)
	After  func(QueryInfo)
}

// BeforeHook invoca f.Before quando não-nil.
func (f QueryHookFunc) BeforeHook(i QueryInfo) {
	if f.Before != nil {
		f.Before(i)
	}
}

// AfterHook invoca f.After quando não-nil.
func (f QueryHookFunc) AfterHook(i QueryInfo) {
	if f.After != nil {
		f.After(i)
	}
}

var _ QueryHook = QueryHookFunc{}

// ── hookRegistry ─────────────────────────────────────────────────────────

// hookRegistry guarda os hooks ativos e o collector interno de métricas.
// É alocado uma vez por raiz (New/Connect) e COMPARTILHADO por ponteiro com
// todos os conns derivados (Tx/Conn), permitindo habilitar métricas depois
// do build e iterar race-free sob RWMutex.
type hookRegistry struct {
	mu  sync.RWMutex
	hs  []QueryHook
	met *MetricsCollector // não-nulo enquanto EnableMetrics estiver ativo
}

// newHookRegistry deriva o registry inicial das Options. SEMPRE aloca (mesmo
// sem hooks): assim EnableMetrics pode anexar coletores depois sem reescrever
// o ponteiro compartilhado por todos os conns derivados (race-free). Métodos
// continuam nil-safe para conns montados literalmente em testes.
func newHookRegistry(o Options) *hookRegistry {
	hs := make([]QueryHook, len(o.QueryHooks))
	copy(hs, o.QueryHooks)
	return &hookRegistry{hs: hs}
}

// add registra mais um hook (EnableMetrics) visível a toda a família de conns.
func (hr *hookRegistry) add(h QueryHook) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.hs = append(hr.hs, h)
}

// remove retira um hook por ponteiro (handle.Close das métricas), preservando
// a ordem dos demais. Hooks registrados só por Options nunca são removidos
// pela lib — o alvo deste método é o collector interno (*MetricsCollector).
func (hr *hookRegistry) remove(target *MetricsCollector) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	for i, existing := range hr.hs {
		if mc, ok := existing.(*MetricsCollector); ok && mc == target {
			hs := make([]QueryHook, 0, len(hr.hs)-1)
			hs = append(hs, hr.hs[:i]...)
			hs = append(hs, hr.hs[i+1:]...)
			hr.hs = hs
			return
		}
	}
}

// snapshot copia a lista atual para iteração fora do lock — hooks lentos não
// seguram escritores.
func (hr *hookRegistry) snapshot() []QueryHook {
	if hr == nil {
		return nil
	}
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	if len(hr.hs) == 0 {
		return nil
	}
	out := make([]QueryHook, len(hr.hs))
	copy(out, hr.hs)
	return out
}

// setObserved troca o collector observado (EnableMetrics/handle.Close).
func (hr *hookRegistry) setObserved(m *MetricsCollector) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.met = m
}

// observed devolve o collector atual (nil-safe).
func (hr *hookRegistry) observed() *MetricsCollector {
	if hr == nil {
		return nil
	}
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	return hr.met
}
