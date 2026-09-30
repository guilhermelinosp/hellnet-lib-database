# hellnet-lib-database

Biblioteca de infraestrutura de banco de dados PostgreSQL-first para Go. Configuração via environment variables, modular, cloud-native. Porta idiomática de [Hellnet.Database](https://github.com/guilhermelinosp/hellnet-dep-database) (.NET).

```
Env vars → Options → pgxpool.Pool (*DB) + database/sql via pgx (*SQLX)
```

---

## 🧒 Entenda com 15 anos

### A analogia

Pense no **PostgreSQL** como um caderno gigante e bem-organizado da escola, onde a turma guarda todas as notas. Escrever nele direto dá trabalho: você precisa saber a página certa, não pode rasurar pela metade e, às vezes, a porta do arquivo onde ele fica trava. Esta biblioteca é o **estagiário super-confiável** que vai até o caderno por você: **sabe o caminho** (endereço, usuário e senha ficam nas variáveis de ambiente), **não perde a página** (controla as conexões por você), usa uma **caneta especial que escreve tudo-ou-nada** — quando precisa salvar uma lista (a *transação*), ou escreve a lista inteira, ou não escreve nada — e, se a porta do arquivo **travar numa tentativa** (erro temporário), **tenta de novo sozinho** algumas vezes antes de te avisar (o *retry*).

### O problema que resolve

- Sem a lib, cada chamada exigiria cuidar de contexto, conexão e detalhes na mão — aqui você configura uma vez e o estagiário faz o resto;
- **Erros temporários** (a rede piscou, o banco reiniciou) são retentados automaticamente, sem você escrever nada;
- A transação garante **all-or-nothing**: se algo falhar no meio, nada fica salvo pela metade.

### Mini-dicionário

| Termo                | Analogia                                                                                               |
|----------------------|--------------------------------------------------------------------------------------------------------|
| **pool**             | o estagiário tem várias bicicletas prontas para você não ficar esperando                                |
| **conexão dedicada** | pegar UMA bicicleta emprestada e usar ela só sua por um tempo                                           |
| **transação**        | a lista do mercado: paga tudo junto ou não leva nada                                                    |
| **query tipada**     | pedir as coisas JÁ organizadas na mochila certa, não num saco solto                                     |
| **retry**            | tropeçou? Levanta e tenta de novo                                                                       |
| **DLQ/não-retry**    | mistura errada não melhora mexendo mais: alguns erros não adiantam repetir, é preciso parar e consertar |

### Primeiras linhas

```go
ctx := context.Background()  // cria o crachá do estagiário: de quem é o pedido e até quando vale
db, err := database.NewWithOptions(ctx) // lê as env vars; use WithInstrumentation para telemetry
```

Linha por linha:

- `ctx := context.Background()` — cria um **contexto**: pense nele como o crachá do estagiário, que diz de quem é o pedido e até quando ele vale (prazos e cancelamentos). Você cria UMA vez, no início da aplicação;
- `db, err := database.NewWithOptions(ctx)` — lê as configurações e abre o pool; o `err` avisa se algo deu errado na contratação.

---

## Instalação

```bash
go get github.com/guilhermelinosp/hellnet-lib-database/database
```

### Hooks locais

Instale os hooks uma vez no checkout antes de criar commits ou fazer push:

```bash
lefthook install
```

O `pre-commit` executa formatação, vet, lint, secrets scan e valida que
`go mod tidy` não altera `go.mod` ou `go.sum`. O `pre-push` repete essa
validação e executa os testes com race detector. Se a validação de módulos
falhar, execute `go mod tidy`, revise as alterações e faça commit dos arquivos.

## Configuração

> *Analogia da seção júnior:* configurar é dar ao estagiário o endereço do caderno (host, porta, senha) — sem endereço, ele não chega lá.

### Via environment variables (recomendado)

```bash
export DATABASE_HOST=localhost
export DATABASE_PORT=5432
export DATABASE_NAME=mydb
export DATABASE_USERNAME=postgres
export DATABASE_PASSWORD=password
```

```go
// O contexto da aplicação é passado UMA VEZ, na construção:
ctx := context.Background() // ou um ctx app-scoped/long-lived

db, err := database.NewWithOptions(ctx)
```

> **Env-first é self-contained.** `OpenFromEnv`/`LoadFromEnv` carregam o `.env`
> automaticamente (via `DATABASE_ENV_FILE`, `ENV_FILE` ou o
> convencional `.env`/`./.env`) usando carregamento local. O chamador
> **não** precisa chamar nenhum loader de `.env` — basta definir as variáveis
> (ou o arquivo) e abrir. Isso espelha o padrão das demais libs Hellnet
> (`hellnet-lib-kafka`, `hellnet-lib-cache`, `hellnet-lib-telemetry`).

### Contexto: uma vez no construtor

O `context.Context` é capturado no `New`/`Connect` e **propagado internamente**
pela lib com os timeouts de cada operação (`CommandTimeout`,
`ConnectionTimeout`). Nenhum método operacional recebe ctx:

```go
ctx := context.Background()
db, err := database.NewWithOptions(ctx) // env-first; passe Options para configuração explícita

n, err := db.Execute("UPDATE orders SET status = $1 WHERE id = $2", "done", id) // sem ctx
page, err := repo.Paginate(spec, 1, 20)                                         // sem ctx
```

> Passe um contexto de **vida longa** (app-scoped). O cancelamento cooperativo
> por-request continua disponível via middleware HTTP (onde a origem é o
> request), não via parâmetro.

### Via options explícitas

```go
db, err := database.NewWithOptions(ctx, database.Options{
    Host:     "pg.internal",
    Database: "orders",
    Username: "app",
    Password: "secret",
})
```

---

## Uso

### Query tipada — SQL puro com pgx

```go
type Order struct {
    ID     int64   `db:"id"`
    Status string  `db:"status"`
}

// Query
pending, err := database.Query[Order](db,
    "SELECT * FROM orders WHERE status = $1", "pending")

// Single row (não é erro quando não encontra)
order, found, err := database.QueryRow[Order](db,
    "SELECT * FROM orders WHERE id = $1", id)

// Execute (insert/update/delete)
affected, err := db.Execute(
    "UPDATE orders SET status = $1 WHERE id = $2", "shipped", id)

// Scalar
count, err := database.Scalar[int64](db, "SELECT COUNT(*) FROM orders")
```

O mapeamento segue as convenções do pgx: campos exportados por nome ou tag `db:"coluna"` (use `db:"-"` para ignorar campos; colunas viram snake_case quando não há tag).

**Contrato de mapeamento:** os SELECTs internos do `Repository[T]` projetam explicitamente as colunas de `T`, então structs parciais funcionam (tolerância estilo Dapper a colunas não mapeadas). Já em SQL cru com `Query[T]`/`QueryRow[T]`, o struct precisa cobrir **todas** as colunas retornadas — liste as colunas desejadas na query ou use um struct completo.

### SQL convencional com sqlx

Para queries e transações convencionais compatíveis com `database/sql`, use a
superfície `SQLX`. Ela usa o driver pgx por baixo e oferece `GetContext`,
`SelectContext`, `NamedExecContext` e os demais helpers do sqlx:

```go
sqlxDB, err := database.OpenSQLXFromEnv(ctx)
if err != nil { /* ... */ }
defer sqlxDB.Close()

var order Order
err = sqlxDB.GetContext(ctx, &order,
    "SELECT id, status FROM orders WHERE id = $1", id)

tx, err := sqlxDB.BeginTxx(ctx, nil)
// Use tx.Commit/Rollback and apenas operações do sqlx nesta transação.
```

`DB` continua sendo a superfície pgx-native para `COPY`, batching,
`LISTEN/NOTIFY`, retry e as transações `database.Tx`. Não misture um
`*sqlx.Tx` com um `*database.Tx`; cada transação pertence ao pool que a criou.

### Transações

> *Analogia:* transação = a lista do mercado — paga tudo junto de uma vez, ou não leva nada (nunca metade).

Commit automático. Se a função retorna erro → rollback automático.

```go
err := db.Transactional(func(tx *database.Tx) error {
    if _, err := tx.Execute(
        "UPDATE accounts SET balance = balance - 100 WHERE id = $1", 1); err != nil {
        return err
    }
    _, err := tx.Execute(
        "UPDATE accounts SET balance = balance + 100 WHERE id = $1", 2)
    return err
})
```

Dentro da transação use os helpers `Tx*` (sem retry — retry parcial dentro de transação seria incorreto):

```go
total, err := database.TxScalar[int64](tx, "SELECT SUM(balance) FROM accounts")
rows, err := database.TxQuery[Order](tx, "SELECT * FROM orders")
one, found, err := database.TxQueryRow[Order](tx, "...", args...)
```

### Conexões dedicadas

> *Analogia:* em vez de pegar qualquer bicicleta do pool a cada volta, você pega UMA emprestada e usa ela só sua por um tempo.

Além do pool (que já multiplexa várias conexões automaticamente), a lib expõe
controle explícito de conexão para três necessidades:

- **Abrir e fechar conexões** (ciclo de vida próprio);
- **Usar múltiplas conexões** (vários `Acquire` em paralelo);
- **Usar uma única conexão para N interações** (pin de sessão).

#### Do pool: `Acquire` / `Close`

`Acquire` pega **uma** conexão do pool e a fixa no `*Conn` até `Close`
(devolvendo-a ao pool). Ideal para um batch de N operações que devem dividir
a mesma sessão, ou para dirigir múltiplas conexões manualmente.

```go
conn, err := db.Acquire()
if err != nil { /* ... */ }
defer func() { _ = conn.Close() }() // devolve ao pool

rows, err := database.ConnQuery[Order](conn,
    "SELECT * FROM orders WHERE status = $1", "pending")
affected, err := conn.Execute(
    "UPDATE orders SET status = $1 WHERE id = $2", "done", id)
```

#### Sem pool: `Connect` / `Close`

`Connect` abre **uma** conexão standalone (sem pool) a partir de options — para
programas curtos, scripts ou quando se quer exatamente uma conexão com controle
total de open/close.

```go
conn, err := database.Connect(ctx, database.Options{
    Host: "pg.internal", Database: "orders", Username: "app", Password: "secret",
})
if err != nil { /* ... */ }
defer func() { _ = conn.Close() }()

count, err := database.ConnScalar[int64](conn, "SELECT COUNT(*) FROM orders")
```

#### Transação sobre conexão dedicada: `Begin` / `Commit` / `Rollback`

`Conn.Begin` inicia uma transação na conexão fixa; o chamador decide quando
fazer `Commit`/`Rollback`. Mantenha o `Conn` aberto até a transação terminar e
só então `Close`.

```go
conn, err := db.Acquire()
if err != nil { /* ... */ }
defer func() { _ = conn.Close() }()

tx, err := conn.Begin()
if err != nil { /* ... */ }

if _, err := tx.Execute(
    "UPDATE accounts SET balance = balance - 100 WHERE id = $1", 1); err != nil {
    _ = tx.Rollback()
    return err
}
if _, err := tx.Execute(
    "UPDATE accounts SET balance = balance + 100 WHERE id = $1", 2); err != nil {
    _ = tx.Rollback()
    return err
}
if err := tx.Commit(); err != nil { return err }
```

> `Conn` (e `Tx`) **não** retentam: reexecutar trabalho parcialmente aplicado
> numa conexão fixa seria incorreto — o chamador decide o retry de toda a
> unidade. Use `conn.Transactional(fn)` para o modo automático
> (commit/rollback) sobre a conexão dedicada.

### Repository Pattern

> *Analogia:* é pedir ao estagiário "traga o pedido 42" — ele já sabe onde está cada ficha no caderno, você não precisa folhear nada.

```go
orders := database.NewRepositoryForTable[Order](db, "orders") // tabela explícita
orders := database.NewRepository[Order](db)                   // ou nome(T), como no .NET typeof(T).Name


order, found, err := orders.GetByID(42)
all, err := orders.GetAll()

spec := database.Specification{
    SQL:     "SELECT * FROM orders WHERE status = $1",
    Args:    []any{"pending"},
    OrderBy: "created_at DESC",
}
matches, err := orders.Find(spec)
page, err := orders.Paginate(spec, 1, 20) // página 1-based + total
n, err := orders.Count(spec)
```

### Resultados tipados

```go
result := database.Success(order, duration)
if result.IsSuccess() { /* use result.Data */ }

page := database.PageResult[Order]{
    Items:      rows,
    TotalCount: 100,
    Page:       1,
    PageSize:   20,
}
fmt.Println(page.HasNextPage())
```

---

## Variáveis de Ambiente

| Variável | Obrigatório | Padrão | Descrição |
|----------|-------------|--------|-----------|
| `DATABASE_HOST` | ❌ | `localhost` | Host ou endpoint `host:porta` do PostgreSQL |
| `DATABASE_PORT` | ❌ | `5432` | Porta |
| `DATABASE_NAME` | ✅ | — | Nome do banco |
| `DATABASE_USERNAME` | ✅ | — | Usuário |
| `DATABASE_PASSWORD` | ✅ | — | Senha |
| `DATABASE_POOL_MIN_SIZE` | ❌ | `10` | Pool mínimo |
| `DATABASE_POOL_MAX_SIZE` | ❌ | `100` | Pool máximo |
| `DATABASE_COMMAND_TIMEOUT` | ❌ | `30s` | Timeout por comando |
| `DATABASE_CONNECTION_TIMEOUT` | ❌ | `15s` | Timeout de conexão |
| `DATABASE_RETRY_ENABLED` | ❌ | `true` | Habilitar retry |
| `DATABASE_RETRY_MAX_COUNT` | ❌ | `3` | Máximo de retry attempts |
| `DATABASE_RETRY_BASE_DELAY` | ❌ | `100ms` | Delay base do backoff |
| `DATABASE_SLOW_QUERY` | ❌ | `500ms` | Limiar de log de query lenta |

---

## Resiliência

> *Analogia:* retry = tropeçou, levanta e tenta de novo; mas mistura errada não melhora mexendo mais (erros permanentes não têm retry).

Retry automático com exponential backoff (`baseDelay << tentativa`). Erros permanentes **não** são retentados:

| SQL State | Erro | Motivo |
|-----------|------|--------|
| `42601` | syntax_error | Bug no código |
| `23505` | unique_violation | Dado duplicado |
| `23503` | foreign_key_violation | Referência inválida |
| `42501` | insufficient_privilege | Permissão negada |
| `42P01` | undefined_table | Tabela não existe |
| `42703` | undefined_column | Coluna não existe |

Desabilitar por env:
```bash
export DATABASE_RETRY_ENABLED=false
```

---

## Arquitetura

```
hellnet-lib-database/database
├── database.go        ← Options (env-first), DB, pool pgxpool
├── core.go            ← Execute, Query[T], QueryRow[T], Scalar[T] (núcleo genérico)
├── connection.go      ← Conn (Acquire/Connect), Conn* helpers, Begin/Transactional
├── transaction.go     ← Tx, Transactional (commit/rollback), helpers Tx*, Commit/Rollback
├── repository.go      ← Repository[T], Specification, paginação
├── results.go         ← Result[T], PageResult[T]
└── resilience.go      ← RetryPolicy + discriminação de SQLSTATE
```

### Equivalências com a porta .NET

| Hellnet.Database (.NET) | hellnet-lib-database (Go) |
|---|---|
| `NpgsqlDataSource` | `pgxpool.Pool` |
| Dapper | `pgx.CollectRows` + tags `db:` |
| `IDatabaseExecutor.QueryAsync<T>` | `database.Query[T](db, …)` |
| `IDatabaseTransaction` | `db.Transactional(…)` |
| `IRepository<T>` / `ISpecification<T>` | `Repository[T]` / `Specification` |
| `DatabaseResult<T>` / `PageResult<T>` | `Result[T]` / `PageResult[T]` |
| Polly + SQLSTATE | `RetryPolicy` + `*pgconn.PgError` |
| `AddHellnetDatabase()` (DI) | `New` / `MustNew` / `OpenFromEnv` |

> Nota: métodos Go não podem declarar parâmetros de tipo, então os helpers genéricos são funções de pacote que recebem `*DB` ou `*Tx`.

---

## Repositórios Relacionados

| Repo | Propósito |
|------|-----------|
| [`hellnet-dep-database`](https://github.com/guilhermelinosp/hellnet-dep-database) | Original .NET |
| [`hellnet-lib-cache`](https://github.com/guilhermelinosp/hellnet-lib-cache) | Multi-layer cache |
| [`hellnet-lib-telemetry`](https://github.com/guilhermelinosp/hellnet-lib-telemetry) | OpenTelemetry + logging |

---

## Testes

```bash
make test           # unitários (sem dependências externas)
make test-race      # unitários com race detector

# Integração contra um PostgreSQL real (build tag `integration`):
export TEST_PG_HOST=localhost TEST_PG_PORT=5432 \
       TEST_PG_USER=postgres TEST_PG_NAME=postgres \
       TEST_PG_PASSWORD=<senha>
go test -tags integration -race ./database/
```

Esses testes permanecem locais, protegidos por build tag e variáveis de
ambiente; a biblioteca não cria workflow de integração no CI.

---

## Observabilidade

Passe uma implementação de `instrument.Instrumentation` — por exemplo, o
cliente do [`hellnet-lib-telemetry`](https://github.com/guilhermelinosp/hellnet-lib-telemetry)
— usando a opção aditiva:

```go
db, err := database.NewWithOptions(ctx, options, database.WithInstrumentation(tel))
```

As APIs ctx-first emitem `db.execute`, `db.query` e `db.scalar` como spans
filhos do chamador. O driver pgx também emite `db.query` para cada statement,
inclusive na superfície `SQLX`; argumentos nunca são registrados e
`HideQueryArgs` também remove o texto SQL. Métricas OTel: `db.client.operation.duration` (`s`),
`db.client.connection.count` (`db.client.connection.state=used|idle`),
`db.client.connection.max`, `db.client.connection.wait_time` (`s`),
`hellnet.db.transactions` (`result=commit|rollback|panic`) e `hellnet.db.retries`.
As métricas do pool usam callback OTel e deixam de observar quando `db.Close()`
é chamado. Hooks que implementam `ContextQueryHook` recebem o contexto da
requisição. `PingContext(ctx)` deve ser registrado pelo serviço em `/ready`.

Para a exposição Prometheus nativa e opcional, chame `EnableMetrics` uma vez
por `DB` e feche o handle durante o desligamento. Ela registra as famílias
`db_queries_total`, `db_query_duration_seconds`, métricas de pool e de
transação; `MetricsHandle.Close` interrompe o sampler, remove o hook e
desregistra os collectors.

```go
handle, err := db.EnableMetrics(prometheus.DefaultRegisterer)
if err != nil { /* ... */ }
defer handle.Close()
```

## Licença

[Apache 2.0](LICENSE)

<!-- release pipeline verification -->

<!-- verify round 2 -->
