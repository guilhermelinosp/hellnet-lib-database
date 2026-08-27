package database

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ── Fakes de runner/pool com linhas reproduzíveis (hooks + métricas) ────────

// hookRow é a linha mapeável usada pelos testes de dispatch: campos na MESMA
// ordem dos field descriptions do fake (garante pareamento posicional do Scan,
// que o mapper namedStructRowScanner faz por ordem de colunas casadas).
type hookRow struct {
	ID     int64  `db:"id"`
	Status string `db:"status"`
}

var hookRowFDs = []pgconn.FieldDescription{
	{Name: "id"},
	{Name: "status"},
}

// fakeRows implementa pgx.Rows sobre linhas em memória. Uma instância é de uso
// único (Next consome) — os runners abaixo carregam uma FACTORY para repetir
// tentativas de retry sem reuso cruzado.
type fakeRows struct {
	fds  []pgconn.FieldDescription
	vals [][]any // cada linha na mesma ordem dos fds
	i    int
	err  error // devolvido por Err()/Values() após esgotar
}

func (r *fakeRows) Next() bool {
	// Semântica pgx: Next avança ANTES do Scan da linha corrente.
	if r.i < len(r.vals) {
		r.i++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.vals) {
		return errors.New("fakeRows: Scan sem Next=true")
	}
	row := r.vals[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: %d destinos para %d valores", len(dest), len(row))
	}
	for j, d := range dest {
		dv := anyPointerValue(d)
		sv := row[j]
		switch tv := dv.(type) {
		case *int64:
			n, ok := sv.(int64)
			if !ok {
				return fmt.Errorf("fakeRows: coluna %d não é int64", j)
			}
			*tv = n
		case *string:
			s, ok := sv.(string)
			if !ok {
				return fmt.Errorf("fakeRows: coluna %d não é string", j)
			}
			*tv = s
		default:
			return fmt.Errorf("fakeRows: destino %T não suportado", d)
		}
	}
	return nil
}

func anyPointerValue(v any) any {
	return v // mantém legibilidade do switch acima
}

func (r *fakeRows) Values() ([]any, error) {
	if r.i == 0 || r.i > len(r.vals) {
		return nil, errors.New("fakeRows: Values sem Next=true")
	}
	vals := r.vals[r.i-1]
	out := make([]any, len(vals))
	copy(out, vals)
	return out, r.err
}

func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fds }
func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT 0") }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

var _ pgx.Rows = (*fakeRows)(nil)

// fakeSingleRow adapta um conjunto de UMA linha ao contrato pgx.Row (usado no
// caminho scalar/query_row da lib).
type fakeSingleRow struct {
	rs  *fakeRows
	scn func(dest ...any) error
}

func newRow(rs *fakeRows) pgx.Row {
	return &fakeSingleRow{
		rs: rs,
		scn: func(dest ...any) error {
			if !rs.Next() {
				if rs.err != nil {
					return rs.err
				}
				return pgx.ErrNoRows
			}
			return rs.Scan(dest...)
		},
	}
}

func (r *fakeSingleRow) Scan(dest ...any) error { return r.scn(dest...) }

// fakeRunnerPool é runner + Pool completo: responde Query/QueryRow/Exec com o
// comportamento programado e grava o último sql/args observado.
type fakeRunnerPool struct {
	mu sync.Mutex

	queryRowsFactory func() *fakeRows
	execRows         int64 // command tag "UPDATE n"
	execErr          error
	queryErr         error // erro imediato do Query (statement-level)

	lastSQL  string
	lastArgs []any
	stats    PoolStats
	acquired int64
	closed   bool
}

func (f *fakeRunnerPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", f.execRows)), nil
}

func (f *fakeRunnerPool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.mu.Lock()
	f.lastSQL, f.lastArgs = sql, args
	err := f.queryErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if f.queryRowsFactory != nil { // mesma fonte para Query e QueryRow (a lib usa Query por baixo de query_row)
		return f.queryRowsFactory(), nil
	}
	return &fakeRows{fds: hookRowFDs, vals: [][]any{{int64(3), "paid"}}}, nil
}

func (f *fakeRunnerPool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.mu.Lock()
	f.lastSQL, f.lastArgs = sql, args
	f.mu.Unlock()
	rs := &fakeRows{fds: hookRowFDs, vals: [][]any{{int64(7), "scalar"}}}
	if f.queryRowsFactory != nil {
		rs = f.queryRowsFactory()
	}
	return newRow(rs)
}

func (f *fakeRunnerPool) Stat() PoolStats { return f.stats }

func (f *fakeRunnerPool) Close() { f.closed = true }

func (f *fakeRunnerPool) Ping(context.Context) error { return nil }

func (f *fakeRunnerPool) Begin(ctx context.Context) (pgx.Tx, error) {
	f.acquired++
	return &fakeTx{}, ctx.Err()
}

var _ Pool = (*fakeRunnerPool)(nil)
var _ runner = (*fakeRunnerPool)(nil)

// newHookTestDB monta um DB de teste completo sobre runner/pool informados
// (o mesmo objeto serve aos dois papéis).
func newHookTestDB(t *testing.T, r *fakeRunnerPool, o Options) *DB {
	t.Helper()
	if o.CommandTimeout == 0 {
		o.CommandTimeout = 100 * time.Millisecond
	}
	if o.ConnectionTimeout == 0 {
		o.ConnectionTimeout = 100 * time.Millisecond
	}
	return &DB{
		conn:  newConn(r, o, context.Background()),
		pool:  r,
		retry: NewRetryPolicy(o.RetryEnabled, o.RetryMaxCount, o.RetryBaseDelay),
	}
}

// ── Recorder de eventos de hook ──────────────────────────────────────────

type hookEvent struct {
	phase string // "before" | "after"
	info  QueryInfo
}

type hookRecorder struct {
	mu     sync.Mutex
	events []hookEvent
}

func (h *hookRecorder) BeforeHook(info QueryInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, hookEvent{"before", info})
}

func (h *hookRecorder) AfterHook(info QueryInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, hookEvent{"after", info})
}

func (h *hookRecorder) snapshot() []hookEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]hookEvent, len(h.events))
	copy(out, h.events)
	return out
}

func (h *hookRecorder) count(phase string) int {
	n := 0
	for _, e := range h.snapshot() {
		if e.phase == phase {
			n++
		}
	}
	return n
}

// panickingHook explode nas duas fases para provar o recover individual.
type panickingHook struct{}

func (panickingHook) BeforeHook(QueryInfo) { panic("hook before exploded") }
func (panickingHook) AfterHook(QueryInfo)  { panic("hook after exploded") }

// ── A) Ordem before→after e conteúdo do QueryInfo ─────────────────────────

func TestHooksOrderAndInfoOnQueryPath(t *testing.T) {
	rec := &hookRecorder{}
	db := newHookTestDB(t, &fakeRunnerPool{}, Options{QueryHooks: []QueryHook{rec}})

	rows, err := Query[hookRow](db, "SELECT id, status FROM orders WHERE id = $1", int64(3))
	if err != nil {
		t.Fatalf("Query err = %v", err)
	}
	if len(rows) != 1 || rows[0].ID != 3 || rows[0].Status != "paid" {
		t.Fatalf("rows mapeadas incorretamente: %+v", rows)
	}

	events := rec.snapshot()
	if len(events) != 2 {
		t.Fatalf("eventos = %d (%+v), want par before/after", len(events), events)
	}
	if events[0].phase != "before" || events[1].phase != "after" {
		t.Fatalf("ordem errada: %s → %s, want before → after", events[0].phase, events[1].phase)
	}
	b, a := events[0].info, events[1].info

	if b.Op != OpQuery || a.Op != OpQuery {
		t.Errorf("ops = %q/%q, want %q/%q", b.Op, a.Op, OpQuery, OpQuery)
	}
	if b.Duration != 0 {
		t.Errorf("before.Duration = %s, want 0 (pre-hook)", b.Duration)
	}
	if b.SQL != "SELECT id, status FROM orders WHERE id = $1" || a.SQL != b.SQL {
		t.Error("SQL divergente entre fases ou não é o executado")
	}
	if len(b.Args) != 1 || b.Args[0] != int64(3) {
		t.Errorf("before.Args = %#v, want [3]", b.Args)
	}
	if a.Err != nil {
		t.Errorf("after.Err = %v, want nil", a.Err)
	}
	if a.Rows != 1 {
		t.Errorf("after.Rows = %d, want 1 (linhas mapeadas)", a.Rows)
	}
}

func TestHooksExecPathAffectedRows(t *testing.T) {
	rec := &hookRecorder{}
	runner := &fakeRunnerPool{execRows: 5}
	db := newHookTestDB(t, runner, Options{QueryHooks: []QueryHook{rec}})

	n, err := db.Execute("UPDATE orders SET status = $1", "paid")
	if err != nil || n != 5 {
		t.Fatalf("Execute = (%d, %v), want (5, nil)", n, err)
	}

	events := rec.snapshot()
	if len(events) != 2 || events[0].phase != "before" || events[1].phase != "after" {
		t.Fatalf("eventos inesperados: %+v", events)
	}
	a := events[1].info
	if a.Op != OpExec || a.Err != nil || a.Rows != 5 {
		t.Errorf("after = {op:%q err:%v rows:%d}, want {exec nil 5}", a.Op, a.Err, a.Rows)
	}
}

func TestHooksQueryRowScalarAndEmptySemantics(t *testing.T) {
	rec := &hookRecorder{}
	db := newHookTestDB(t, &fakeRunnerPool{}, Options{QueryHooks: []QueryHook{rec}})

	row, found, err := QueryRow[hookRow](db, "SELECT id, status FROM orders WHERE id = $1", int64(7))
	// O caminho query_row internamente usa runner.Query (≤1 linha); o
	// resultado carrega a primeira linha servida pelo fake no canal Query.
	if err != nil || !found || row.ID != 3 || row.Status != "paid" {
		t.Fatalf("QueryRow = (%+v,%t,%v)", row, found, err)
	}
	// Scalar consome UMA coluna via QueryRow().Scan: runner dedicado com
	// rows de coluna única (pareamento posicional 1↔1).
	scalarRec := &hookRecorder{}
	scalarDB := newHookTestDB(t, &fakeRunnerPool{
		queryRowsFactory: func() *fakeRows {
			return &fakeRows{
				fds:  []pgconn.FieldDescription{{Name: "status"}},
				vals: [][]any{{"scalar"}},
			}
		},
	}, Options{QueryHooks: []QueryHook{scalarRec}})
	value, err := Scalar[string](scalarDB, "SELECT status FROM orders")
	if err != nil || value != "scalar" {
		t.Fatalf("Scalar = (%q,%v), want (\"scalar\",nil)", value, err)
	}
	for _, e := range scalarRec.snapshot() {
		if e.info.Op != OpScalar {
			t.Errorf("op = %q, want %q", e.info.Op, OpScalar)
		}
	}

	events := rec.snapshot()
	var queryRowAfter *hookEvent
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e.phase == "after" && e.info.Op == OpQueryRow {
			queryRowAfter = &events[i]
			break
		}
	}
	if queryRowAfter == nil || queryRowAfter.info.Rows != 1 {
		t.Errorf("query_row after Rows, want 1")
	}

	// Resultado vazio NÃO é erro para os hooks (semântica found=false).
	emptyDB := newHookTestDB(t, &fakeRunnerPool{
		queryRowsFactory: func() *fakeRows { return &fakeRows{fds: hookRowFDs} },
	}, Options{QueryHooks: []QueryHook{rec}})
	_, found, err = QueryRow[hookRow](emptyDB, "SELECT id, status FROM orders WHERE id = 0")
	if err != nil || found {
		t.Fatalf("linha vazia: (found=%t err=%v), want (false,nil)", found, err)
	}
	events = rec.snapshot()
	last := events[len(events)-1]
	if last.phase != "after" || last.info.Op != OpQueryRow || last.info.Err != nil || last.info.Rows != 0 {
		t.Errorf("after vazio = %+v, want Err=nil Rows=0 op=query_row", last.info)
	}
}

func TestHooksArgsHiding(t *testing.T) {
	rec := &hookRecorder{}
	hidden := newHookTestDB(t, &fakeRunnerPool{}, Options{
		QueryHooks:    []QueryHook{rec},
		HideQueryArgs: true,
	})
	if _, err := hidden.Execute("INSERT INTO t VALUES ($1, $2)", "secret", 42); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, e := range rec.snapshot() {
		if e.info.Args != nil {
			t.Fatalf("%s.Args = %#v com HideQueryArgs=true, want nil", e.phase, e.info.Args)
		}
	}

	visible := &hookRecorder{}
	open := newHookTestDB(t, &fakeRunnerPool{execRows: 1}, Options{QueryHooks: []QueryHook{visible}})
	if _, err := open.Execute("INSERT INTO t VALUES ($1, $2)", "secret", 42); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, e := range visible.snapshot() {
		got := e.info.Args
		if len(got) != 2 || got[0] != "secret" || got[1] != 42 {
			t.Errorf("%s.Args = %#v, want os argumentos originais", e.phase, got)
		}
	}
}

func TestHookPanicIsolation(t *testing.T) {
	rec := &hookRecorder{}
	db := newHookTestDB(t, &fakeRunnerPool{execRows: 1}, Options{
		QueryHooks: []QueryHook{panickingHook{}, rec},
	})

	// Pânico em Before E After de um hook não pode quebrar a operação nem
	// impedir o recorder (em qualquer posição da lista).
	n, err := db.Execute("UPDATE x SET y = 1")
	if err != nil || n != 1 {
		t.Fatalf("Execute sobreviveu? (%d, %v), want (1, nil)", n, err)
	}
	events := rec.snapshot()
	if len(events) != 2 || events[0].phase != "before" || events[1].phase != "after" {
		t.Fatalf("recorder recebeu %+v, want par before/after intacto", events)
	}

	// Ordem invertida: pânico ANTES do recorder na lista de Before.
	rec2 := &hookRecorder{}
	panicky := newHookTestDB(t, &fakeRunnerPool{execRows: 1}, Options{
		QueryHooks: []QueryHook{panickingHook{}, rec2, panickingHook{}},
	})
	if _, err := panicky.Execute("UPDATE x SET y = 1"); err != nil {
		t.Fatalf("segundo Execute falhou: %v", err)
	}
	if rec2.count("after") != 1 {
		t.Errorf("AfterHook do recorder chamado %d vezes, want 1 (fases independentes)", rec2.count("after"))
	}
}

// B) Caminhos de erro — incluindo erros de CONTEXTO — sempre disparam After.
func TestHooksFireOnStatementAndContextErrors(t *testing.T) {
	t.Run("statement error exec", func(t *testing.T) {
		rec := &hookRecorder{}
		db := newHookTestDB(t, &fakeRunnerPool{execErr: errors.New("boom")},
			Options{QueryHooks: []QueryHook{rec}})
		if _, err := db.Execute("UPDATE x SET y = 1"); err == nil {
			t.Fatal("esperava erro do runner")
		}
		last := rec.snapshot()[1].info
		if last.Op != OpExec || last.Err == nil || last.Rows != rowsUnknown {
			t.Errorf("after = {op:%q err:%v rows:%d}, want erro + rows desconhecido", last.Op, last.Err, last.Rows)
		}
	})

	t.Run("context canceled query", func(t *testing.T) {
		rec := &hookRecorder{}
		db := newHookTestDB(t, &fakeRunnerPool{queryErr: context.Canceled},
			Options{QueryHooks: []QueryHook{rec}})
		if _, err := Query[hookRow](db, "SELECT 1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled propagado", err)
		}
		last := rec.snapshot()[1].info
		if last.Err == nil || statusLabel(last.Err) != metricStatusCanceled {
			t.Errorf("depois de cancelamento: {err:%v}, want label %q", last.Err, metricStatusCanceled)
		}
	})
}

// D) Retry dispara o par before/after POR TENTATIVA (comportamento documentado
// no contrato de QueryHook).
func TestHooksFirePerRetryAttempt(t *testing.T) {
	rec := &hookRecorder{}
	pool := &fakeRunnerPool{execRows: 2}
	seq := &flakyExecRunner{inner: pool, failFirst: true}
	db := &DB{
		conn: newConn(seq, Options{
			CommandTimeout:    100 * time.Millisecond,
			ConnectionTimeout: 100 * time.Millisecond,
			QueryHooks:        []QueryHook{rec},
		}, context.Background()),
		pool:  pool,
		retry: NewRetryPolicy(true, 3, time.Millisecond),
	}

	n, err := db.Execute("UPDATE flaky SET done = true")
	if err != nil || n != 2 {
		t.Fatalf("Execute = (%d, %v), want recuperado no retry (2, nil)", n, err)
	}

	events := rec.snapshot()
	if len(events) != 4 {
		t.Fatalf("eventos = %d, want 4 (2 tentativas × par before/after): %+v", len(events), events)
	}
	wantPhases := []string{"before", "after", "before", "after"}
	for i, e := range events {
		if e.phase != wantPhases[i] {
			t.Fatalf("fase[%d] = %s, want %s", i, e.phase, wantPhases[i])
		}
	}
	firstFail := events[1].info
	if firstFail.Err == nil || firstFail.Rows != rowsUnknown {
		t.Errorf("tentativa 1 after = {err:%v rows:%d}, want erro + rows desconhecido", firstFail.Err, firstFail.Rows)
	}
	finalOK := events[3].info
	if finalOK.Err != nil || finalOK.Rows != 2 {
		t.Errorf("tentativa 2 after = {err:%v rows:%d}, want sucesso + 2 linhas", finalOK.Err, finalOK.Rows)
	}
	if firstFail.SQL != finalOK.SQL {
		t.Errorf("SQL diverge entre tentativas: %q vs %q", firstFail.SQL, finalOK.SQL)
	}
}

// flakyExecRunner falha a PRIMEIRA execução e depois passa — provando o par
// de hooks por tentativa dentro do loop de retry da própria DB.Execute.
type flakyExecRunner struct {
	inner     *fakeRunnerPool
	failFirst bool
	mu        sync.Mutex
}

func (f *flakyExecRunner) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFirst {
		f.failFirst = false
		return pgconn.CommandTag{}, errors.New("transient connection blip")
	}
	return f.inner.Exec(ctx, sql, args...)
}

func (f *flakyExecRunner) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f.inner.Query(ctx, sql, args...)
}

func (f *flakyExecRunner) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f.inner.QueryRow(ctx, sql, args...)
}

var _ runner = (*flakyExecRunner)(nil)
