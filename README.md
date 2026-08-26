# Hellnet Database

Biblioteca de infraestrutura de banco de dados PostgreSQL-first para Go. Configuração via environment variables, modular, cloud-native. Porta idiomática de [Hellnet.Database](https://github.com/guilhermelinosp/hellnet-dep-database) (.NET).

```
Env vars → Options → pgxpool.Pool → *DB / Repository[T]
```

---

## Instalação

```bash
go get github.com/guilhermelinosp/hellnet-lib-database/database
```

## Configuração

### Via environment variables (recomendado)

```bash
export HELLNET_DATABASE_HOST=localhost
export HELLNET_DATABASE_PORT=5432
export HELLNET_DATABASE_NAME=mydb
export HELLNET_DATABASE_USERNAME=postgres
export HELLNET_DATABASE_PASSWORD=password
```

```go
db, err := database.OpenFromEnv()
```

### Via options explícitas

```go
db, err := database.New(database.Options{
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
pending, err := database.Query[Order](ctx, db,
    "SELECT * FROM orders WHERE status = $1", "pending")

// Single row (não é erro quando não encontra)
order, found, err := database.QueryRow[Order](ctx, db,
    "SELECT * FROM orders WHERE id = $1", id)

// Execute (insert/update/delete)
affected, err := db.Execute(ctx,
    "UPDATE orders SET status = $1 WHERE id = $2", "shipped", id)

// Scalar
count, err := database.Scalar[int64](ctx, db, "SELECT COUNT(*) FROM orders")
```

O mapeamento segue as convenções do pgx: campos exportados por nome ou tag `db:"coluna"` (use `db:"-"` para ignorar campos; colunas viram snake_case quando não há tag).

**Contrato de mapeamento:** os SELECTs internos do `Repository[T]` projetam explicitamente as colunas de `T`, então structs parciais funcionam (tolerância estilo Dapper a colunas não mapeadas). Já em SQL cru com `Query[T]`/`QueryRow[T]`, o struct precisa cobrir **todas** as colunas retornadas — liste as colunas desejadas na query ou use um struct completo.

### Transações

Commit automático. Se a função retorna erro → rollback automático.

```go
err := db.Transactional(ctx, func(ctx context.Context, tx *database.Tx) error {
    if _, err := tx.Execute(ctx,
        "UPDATE accounts SET balance = balance - 100 WHERE id = $1", 1); err != nil {
        return err
    }
    _, err := tx.Execute(ctx,
        "UPDATE accounts SET balance = balance + 100 WHERE id = $1", 2)
    return err
})
```

Dentro da transação use os helpers `Tx*` (sem retry — retry parcial dentro de transação seria incorreto):

```go
total, err := database.TxScalar[int64](ctx, tx, "SELECT SUM(balance) FROM accounts")
rows, err := database.TxQuery[Order](ctx, tx, "SELECT * FROM orders")
one, found, err := database.TxQueryRow[Order](ctx, tx, "...", args...)
```

### Repository Pattern

```go
orders := database.NewRepositoryForTable[Order](db, "orders") // tabela explícita
orders := database.NewRepository[Order](db)                   // ou nome(T), como no .NET typeof(T).Name


order, found, err := orders.GetByID(ctx, 42)
all, err := orders.GetAll(ctx)

spec := database.Specification{
    SQL:     "SELECT * FROM orders WHERE status = $1",
    Args:    []any{"pending"},
    OrderBy: "created_at DESC",
}
matches, err := orders.Find(ctx, spec)
page, err := orders.Paginate(ctx, spec, 1, 20) // página 1-based + total
n, err := orders.Count(ctx, spec)
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
| `HELLNET_DATABASE_HOST` | ❌ | `localhost` | Host do PostgreSQL |
| `HELLNET_DATABASE_PORT` | ❌ | `5432` | Porta |
| `HELLNET_DATABASE_NAME` | ✅ | — | Nome do banco |
| `HELLNET_DATABASE_USERNAME` | ✅ | — | Usuário |
| `HELLNET_DATABASE_PASSWORD` | ✅ | — | Senha |
| `HELLNET_DATABASE_POOL_MIN_SIZE` | ❌ | `10` | Pool mínimo |
| `HELLNET_DATABASE_POOL_MAX_SIZE` | ❌ | `100` | Pool máximo |
| `HELLNET_DATABASE_COMMAND_TIMEOUT_SECONDS` | ❌ | `30` | Timeout por comando |
| `HELLNET_DATABASE_CONNECTION_TIMEOUT_SECONDS` | ❌ | `15` | Timeout de conexão |
| `HELLNET_DATABASE_RETRY_ENABLED` | ❌ | `true` | Habilitar retry |
| `HELLNET_DATABASE_RETRY_MAX_COUNT` | ❌ | `3` | Máximo de retry attempts |
| `HELLNET_DATABASE_RETRY_BASE_DELAY_MS` | ❌ | `100` | Delay base do backoff |
| `HELLNET_DATABASE_SLOW_QUERY_MS` | ❌ | `500` | Limiar de log de query lenta |

---

## Resiliência

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
export HELLNET_DATABASE_RETRY_ENABLED=false
```

---

## Arquitetura

```
hellnet-lib-database/database
├── database.go        ← Options (env-first), DB, pool pgxpool
├── executor.go        ← Execute, Query[T], QueryRow[T], Scalar[T]
├── transaction.go     ← Transactional (commit/rollback), helpers Tx*
├── repository.go      ← Repository[T], Specification, paginação
├── results.go         ← Result[T], PageResult[T]
└── resilience.go      ← RetryPolicy + discriminação de SQLSTATE
```

### Equivalências com a porta .NET

| Hellnet.Database (.NET) | hellnet-lib-database (Go) |
|---|---|
| `NpgsqlDataSource` | `pgxpool.Pool` |
| Dapper | `pgx.CollectRows` + tags `db:` |
| `IDatabaseExecutor.QueryAsync<T>` | `database.Query[T](ctx, db, …)` |
| `IDatabaseTransaction` | `db.Transactional(…)` |
| `IRepository<T>` / `ISpecification<T>` | `Repository[T]` / `Specification` |
| `DatabaseResult<T>` / `PageResult<T>` | `Result[T]` / `PageResult[T]` |
| Polly + SQLSTATE | `RetryPolicy` + `*pgconn.PgError` |
| `AddHellnetDatabase()` (DI) | `New` / `MustNew` / `OpenFromEnv` |

> Nota: métodos Go não podem declarar parâmetros de tipo, então os helpers genéricos são funções de pacote que recebem `*DB` ou `*Tx`.

---

## Observabilidade

Sem instrumentação própria. Use OpenTelemetry padrão para `database/sql`/pgx e delegue health checks ao [`hellnet-lib-telemetry`](https://github.com/guilhermelinosp/hellnet-lib-telemetry). Queries acima do limiar `SLOW_QUERY_MS` geram log estruturado via `log/slog`.

---

## Repositórios Relacionados

| Repo | Propósito |
|------|-----------|
| [`hellnet-dep-database`](https://github.com/guilhermelinosp/hellnet-dep-database) | Original .NET |
| [`hellnet-lib-cache`](https://github.com/guilhermelinosp/hellnet-lib-cache) | Multi-layer cache |
| [`hellnet-lib-environments`](https://github.com/guilhermelinosp/hellnet-lib-environments) | Env vars + .env compartilhado |
| [`hellnet-lib-telemetry`](https://github.com/guilhermelinosp/hellnet-lib-telemetry) | OpenTelemetry + logging |

---

## Testes

```bash
make test           # unitários (sem dependências externas)
make test-race      # unitários com race detector

# Integração contra um PostgreSQL real (build tag `integration`):
export HELLNET_TEST_PG_HOST=localhost HELLNET_TEST_PG_PORT=5432 \
       HELLNET_TEST_PG_USER=postgres HELLNET_TEST_PG_NAME=postgres \
       HELLNET_TEST_PG_PASSWORD=<senha>
go test -tags integration -race ./database/
```

No CI, o workflow `integration` sobe um container `postgres:16` e roda esses
mesmos testes automaticamente a cada PR.

---

## Licença

[Apache 2.0](LICENSE)
