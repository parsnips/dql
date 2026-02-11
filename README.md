# DynamoDB Local in Go — Implementation Plan

## Project Codename: **dynamo-go-local** (or `dql` for short)

---

## 1. Vision & Goals

**Build a drop-in replacement for DynamoDB Local that is:**

- API-compatible at the wire protocol level (same JSON over HTTP)
- Blazing fast — sub-millisecond single-item ops, parallel-safe for test suites
- Lock-free at the global level — concurrency is per-table or per-partition
- Backed by SQLite (default) with a pluggable storage interface
- Feature-complete: Streams, PartiQL, Transactions, GSIs/LSIs, TTL, Batch ops

**Non-goals (initially):**

- Full IAM / auth simulation (accept any credentials)
- Capacity unit tracking / throttling simulation
- Cross-region replication

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    HTTP / CBOR Layer                     │
│         (net/http, reads X-Amz-Target header)           │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                  API Dispatcher                          │
│   Routes to handler by operation name (PutItem, etc.)   │
└──────────────────────┬──────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────┐
│                 Table Manager                            │
│   Table metadata, schema registry, in-memory catalog    │
│   RWMutex per table for DDL; read-lock for DML          │
└──────────┬───────────────────────┬──────────────────────┘
           │                       │
┌──────────▼──────────┐ ┌─────────▼─────────────────────┐
│  Expression Engine   │ │       PartiQL Engine           │
│  Filter, Projection, │ │  Lexer → Parser → Planner →   │
│  Condition, Update,  │ │  Executor (reuses expr engine) │
│  KeyCondition exprs  │ │                                │
└──────────┬──────────┘ └─────────┬─────────────────────┘
           │                       │
┌──────────▼───────────────────────▼──────────────────────┐
│                 Storage Interface                        │
│                                                          │
│  type StorageEngine interface {                          │
│      PutItem(table, item) error                          │
│      GetItem(table, key) (Item, error)                   │
│      DeleteItem(table, key) error                        │
│      Query(table, keyCondition, filter) ([]Item, error)  │
│      Scan(table, filter) ([]Item, error)                 │
│      ...                                                 │
│  }                                                       │
└──────────┬──────────────────────┬───────────────────────┘
           │                      │
┌──────────▼──────────┐ ┌────────▼────────────────────────┐
│  SQLite Backend      │ │  Future: Postgres/Vitess/Memory │
│  (mattn/go-sqlite3   │ │                                 │
│   or modernc.org/    │ │                                 │
│   sqlite — pure Go)  │ │                                 │
└──────────┬──────────┘ └─────────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────┐
│              Partition Manager (SQLite)                   │
│  1 SQLite DB per table (or per partition range)          │
│  WAL mode, per-DB connection pool, no global lock       │
└─────────────────────────────────────────────────────────┘
           │
┌──────────▼──────────────────────────────────────────────┐
│                  Streams Engine                           │
│  CDC from storage layer → in-memory ring buffer          │
│  GetRecords / GetShardIterator / DescribeStream          │
└─────────────────────────────────────────────────────────┘
```

---

## 3. Concurrency Model (The Key Differentiator)

The whole point is to eliminate the global lock. Here's the strategy:

### 3.1 Table-Level

| Operation | Lock Type |
|---|---|
| CreateTable / DeleteTable / UpdateTable | Exclusive lock on table catalog |
| PutItem / GetItem / DeleteItem / Query / Scan | Read lock on catalog, **no table-wide lock** |

### 3.2 Partition-Level (SQLite Backend)

- Each table gets its own SQLite database file (or set of files).
- SQLite in **WAL mode** allows concurrent reads + single writer per DB.
- For write-heavy parallel tests, consider **sharding into N SQLite files per table** keyed by `hash(partition_key) % N`. This gives N independent write locks.
- Default shard count: 4 (configurable). For most test workloads even 1 is fine with WAL mode.

### 3.3 Transactions

- `TransactWriteItems` / `TransactGetItems` can span tables.
- Use ordered lock acquisition (sort by table name, then by key) to prevent deadlocks.
- Each item lock is a fine-grained mutex from a striped lock pool (e.g., 1024 mutexes, hash key to index).

---

## 4. Module Breakdown & Implementation Phases

### 4.0 Delivery Sequence (Refined for Speed)

To de-risk compatibility and move faster, build in this order:

1. **Compatibility harness first** (AWS SDK v2 client + golden tests against DynamoDB Local and `dql`)
2. **API skeleton + in-memory backend** for fast iteration and deterministic tests
3. **SQLite backend** for persistence and concurrency behavior
4. **Expression support** (condition/filter/projection/update)
5. **Batch + transactions**
6. **Streams**
7. **PartiQL subset**
8. **Advanced features** (TTL, backups, stubs, polish)

Open-source acceleration strategy:

- Reuse AWS SDK v2 DynamoDB request/response model types instead of defining parallel DTOs.
- Reuse SDK AttributeValue JSON helpers for wire compatibility.
- Reuse maintained parser libraries for expression/SQL subset parsing where practical.
- Keep handwritten logic for the DynamoDB-specific planning and execution path.

### Phase 1: Compatibility Harness + Core CRUD (MVP — ~2-3 weeks)

> Goal: Pass the AWS SDK `PutItem` / `GetItem` / `DeleteItem` / `Query` / `Scan` round-trip.

#### 4.1 HTTP Layer (`pkg/api/`)

- `net/http` server, parse `X-Amz-Target` header for operation routing (e.g., `DynamoDB_20120810.PutItem`)
- JSON request/response marshaling matching the DynamoDB JSON wire format
- Prefer `aws-sdk-go-v2/service/dynamodb/types` request/response model types where possible
- Use `aws-sdk-go-v2/feature/dynamodb/attributevalue` helpers for DynamoDB JSON AttributeValue translation
- Error response formatting (DynamoDB-style `__type` error codes)

```
Files:
  pkg/api/server.go          — HTTP server, middleware, routing
  pkg/api/dispatcher.go      — X-Amz-Target → handler map
  pkg/api/errors.go          — DynamoDB error types & serialization
  pkg/api/types.go           — Request/Response structs for each operation
```

#### 4.2 Attribute Value Model (`pkg/types/`)

- Internal normalized representation of DynamoDB items for storage/query execution
- Bridge to/from SDK AttributeValue model types at the API boundary
- Comparators for sort keys (binary, string, number comparison)
- Type system: S, N, B, SS, NS, BS, L, M, BOOL, NULL

```
Files:
  pkg/types/attributevalue.go — Core AV type + marshal/unmarshal
  pkg/types/compare.go        — Key comparison functions
  pkg/types/item.go            — Item (map[string]AttributeValue) helpers
```

#### 4.3 Table Manager (`pkg/table/`)

- In-memory catalog of tables (name → schema + config)
- CreateTable / DeleteTable / DescribeTable / ListTables / UpdateTable
- Schema validation (key schema, GSI/LSI definitions, attribute definitions)
- `sync.RWMutex` on the catalog; table creation/deletion takes write lock

```
Files:
  pkg/table/catalog.go    — Table registry, CRUD on table definitions
  pkg/table/schema.go     — Key schema, index definitions
  pkg/table/validate.go   — Input validation
```

#### 4.4 Storage Interface (`pkg/storage/`)

```go
type Engine interface {
    CreateTable(ctx context.Context, def TableDef) error
    DeleteTable(ctx context.Context, name string) error

    PutItem(ctx context.Context, table string, item Item, opts PutOpts) (*Item, error)
    GetItem(ctx context.Context, table string, key Key, opts GetOpts) (*Item, error)
    DeleteItem(ctx context.Context, table string, key Key, opts DeleteOpts) (*Item, error)
    UpdateItem(ctx context.Context, table string, key Key, opts UpdateOpts) (*Item, error)

    Query(ctx context.Context, table string, params QueryParams) (*QueryResult, error)
    Scan(ctx context.Context, table string, params ScanParams) (*ScanResult, error)
}
```

- `PutOpts` / `DeleteOpts` etc. carry condition expressions, return values config
- The old item is returned when `ReturnValues` is set (needed for streams too)

```
Files:
  pkg/storage/engine.go   — Interface definition
  pkg/storage/types.go    — Opts, Params, Result types
```

#### 4.5 SQLite Backend (`pkg/storage/sqlite/`)

**Schema per table DB:**

```sql
-- Main item table
CREATE TABLE items (
    pk   BLOB NOT NULL,   -- marshaled partition key
    sk   BLOB NOT NULL,   -- marshaled sort key (empty blob if table has no SK)
    data BLOB NOT NULL,   -- msgpack/cbor encoded full item
    PRIMARY KEY (pk, sk)
);

-- One table per GSI
CREATE TABLE gsi_{index_name} (
    gsi_pk  BLOB NOT NULL,
    gsi_sk  BLOB NOT NULL,
    pk      BLOB NOT NULL,  -- back-reference to main table
    sk      BLOB NOT NULL,
    data    BLOB NOT NULL,  -- projected attributes
    PRIMARY KEY (gsi_pk, gsi_sk, pk, sk)
);

-- LSIs share the main table's partition key
CREATE TABLE lsi_{index_name} (
    pk      BLOB NOT NULL,
    lsi_sk  BLOB NOT NULL,
    sk      BLOB NOT NULL,  -- original sort key for back-reference
    data    BLOB NOT NULL,
    PRIMARY KEY (pk, lsi_sk, sk)
);
```

**Key decisions:**

- Use `modernc.org/sqlite` (pure Go, CGo-free) for easy cross-compilation and no CGo headaches. Benchmark against `mattn/go-sqlite3` and let users choose via build tag.
- Item data encoded as **CBOR** (matches DynamoDB's internal format, fast, compact). Use `fxamacker/cbor`.
- Sort keys encoded with **order-preserving binary encoding** so SQLite's `BLOB` comparison matches DynamoDB's sort order.
- WAL mode + `PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`
- Connection pooling: separate read pool (N connections) and write pool (1 connection per shard) per SQLite DB.

```
Files:
  pkg/storage/sqlite/backend.go     — Engine implementation
  pkg/storage/sqlite/db.go          — DB lifecycle, pooling, pragmas
  pkg/storage/sqlite/schema.go      — DDL generation for tables/indexes
  pkg/storage/sqlite/keyencode.go   — Order-preserving key encoding
  pkg/storage/sqlite/query.go       — SQL generation for Query/Scan
  pkg/storage/sqlite/shard.go       — Partition sharding logic
```

#### 4.6 Operation Handlers (`pkg/handlers/`)

One file per DynamoDB API operation:

```
Files:
  pkg/handlers/put_item.go
  pkg/handlers/get_item.go
  pkg/handlers/delete_item.go
  pkg/handlers/update_item.go
  pkg/handlers/query.go
  pkg/handlers/scan.go
  pkg/handlers/create_table.go
  pkg/handlers/delete_table.go
  pkg/handlers/describe_table.go
  pkg/handlers/list_tables.go
  pkg/handlers/update_table.go
```

---

### Phase 2: Expressions (~1-2 weeks)

DynamoDB has 5 expression types. They all share a similar grammar but are used in different contexts.

#### 4.7 Expression Engine (`pkg/expression/`)

**Approach:** Build a single lexer/parser that handles the DynamoDB expression grammar, then produce an AST that can be evaluated in different contexts.

| Expression Type | Used By | Evaluates To |
|---|---|---|
| KeyConditionExpression | Query | Boolean (filters on PK/SK) |
| FilterExpression | Query, Scan | Boolean (post-read filter) |
| ConditionExpression | PutItem, DeleteItem, UpdateItem | Boolean (precondition) |
| ProjectionExpression | GetItem, Query, Scan | Attribute path list |
| UpdateExpression | UpdateItem | SET/REMOVE/ADD/DELETE actions |

**Expression attribute names** (`#name` → actual name) and **expression attribute values** (`:val` → AV) are substituted during parsing.

```
Files:
  pkg/expression/lexer.go       — Tokenizer
  pkg/expression/parser.go      — Recursive descent parser → AST
  pkg/expression/ast.go         — AST node types
  pkg/expression/evaluator.go   — Evaluate filter/condition against an item
  pkg/expression/projection.go  — Apply projection to an item
  pkg/expression/update.go      — Apply SET/REMOVE/ADD/DELETE to an item
  pkg/expression/functions.go   — Built-in functions (attribute_exists, begins_with, contains, size, etc.)
  pkg/expression/keyeval.go     — Extract key conditions for storage layer push-down
```

---

### Phase 3: Batch & Transactions (~1 week)

#### 4.8 Batch Operations (`pkg/handlers/`)

- `BatchWriteItem` — up to 25 puts/deletes across tables. Execute concurrently per table.
- `BatchGetItem` — up to 100 gets across tables. Execute concurrently.
- Handle `UnprocessedItems` / `UnprocessedKeys` (in local mode, always process all — but respect the interface).

#### 4.9 Transactions (`pkg/handlers/`)

- `TransactWriteItems` — up to 100 actions (Put, Delete, Update, ConditionCheck) with ACID.
- `TransactGetItems` — up to 100 gets with read consistency.
- **Implementation:** Acquire ordered fine-grained locks, validate all conditions, apply all writes, release locks.
- Use a `StripedLock` (array of N mutexes, hash item key to index) to avoid per-key mutex allocation.

```
Files:
  pkg/handlers/batch_write_item.go
  pkg/handlers/batch_get_item.go
  pkg/handlers/transact_write_items.go
  pkg/handlers/transact_get_items.go
  pkg/concurrency/striped_lock.go
```

---

### Phase 4: DynamoDB Streams (~1-2 weeks)

#### 4.10 Streams Engine (`pkg/streams/`)

**Architecture:**

- Each table with streams enabled gets a **change log** — an append-only in-memory ring buffer (configurable max size, default 1M records or 24h simulated).
- On every write (Put/Delete/Update), the storage layer emits a `StreamRecord` to the table's change log.
- Shards are simulated: 1 shard per table (or per partition range if you want realism).
- `GetShardIterator` returns an opaque cursor (encoded offset in the ring buffer).
- `GetRecords` reads from cursor, returns records + next cursor.

**Stream view types:** `KEYS_ONLY`, `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES` — controlled at table level, determines what gets captured in the stream record.

```
Files:
  pkg/streams/changelog.go       — Ring buffer per table
  pkg/streams/record.go          — StreamRecord type
  pkg/streams/shard.go           — Shard management
  pkg/streams/handlers.go        — DescribeStream, GetShardIterator, GetRecords, ListStreams
```

**Integration point:**

The storage engine's write methods return the old item (if applicable). The handler layer constructs a `StreamRecord` and pushes it to the changelog. This keeps streams decoupled from storage.

---

### Phase 5: PartiQL (~1-2 weeks)

#### 4.11 PartiQL Engine (`pkg/partiql/`)

DynamoDB supports a subset of PartiQL:

- `SELECT` → maps to Query or Scan
- `INSERT` → maps to PutItem
- `UPDATE` → maps to UpdateItem
- `DELETE` → maps to DeleteItem

**Approach:** Write a dedicated PartiQL lexer/parser for the DynamoDB subset (not full PartiQL — it's much simpler). The planner converts the AST into calls to the same storage interface.

Refined approach for speed:

- Start with a DynamoDB-safe subset parser using `vitess.io/vitess/go/vt/sqlparser` for `SELECT`/`INSERT`/`UPDATE`/`DELETE`.
- Translate parsed statements into existing handler/storage calls.
- Keep unsupported PartiQL constructs explicit and return clear validation errors.
- Re-evaluate full PartiQL parser integration later (for example, sidecar/WASM bridge to official parser implementations) only if parity gaps justify the complexity.

```
Files:
  pkg/partiql/lexer.go
  pkg/partiql/parser.go
  pkg/partiql/ast.go
  pkg/partiql/planner.go     — Converts PartiQL AST → storage calls
  pkg/partiql/executor.go    — Executes planned operations
  pkg/handlers/execute_statement.go
  pkg/handlers/batch_execute_statement.go
  pkg/handlers/execute_transaction.go
```

---

### Phase 6: Advanced Features (~2-3 weeks)

#### 4.12 TTL (`pkg/ttl/`)

- Background goroutine that periodically scans for expired items.
- Uses a SQLite index on the TTL attribute for efficient scanning.
- Deletes expired items and emits stream records (with `userIdentity: {type: "Service", principalId: "dynamodb.amazonaws.com"}`).

#### 4.13 Table Features

- `UpdateTable` — modify GSIs (add/remove), modify stream settings, modify TTL.
- `DescribeTable` — item count, size estimates, index status.
- `UpdateTimeToLive` / `DescribeTimeToLive`
- `TagResource` / `UntagResource` / `ListTagsOfResource` (in-memory tag store)

#### 4.14 Pagination

- `Query` and `Scan` return `LastEvaluatedKey` when results exceed 1MB or `Limit`.
- `ExclusiveStartKey` resumes from where the previous call left off.
- SQLite implementation: encode the last-seen key as the start key for a `WHERE (pk, sk) > (?, ?)` clause.

#### 4.15 Additional Operations

- `DescribeEndpoints`
- `DescribeLimits`
- `UpdateContinuousBackups` / `DescribeContinuousBackups` (stub)
- `CreateBackup` / `DeleteBackup` / `DescribeBackup` / `ListBackups` / `RestoreTableFromBackup` (could actually implement with SQLite file copy)

---

## 5. Pluggable Storage Layer

### Interface Design

The `Engine` interface (§4.4) is the abstraction boundary. Additional backends can be added:

| Backend | Use Case | Notes |
|---|---|---|
| `sqlite` (default) | Local dev, CI/CD, unit tests | Pure Go, zero dependencies |
| `memory` | Ultra-fast ephemeral tests | Map-based, no persistence |
| `postgres` | Shared test environments | Single Postgres for team |
| `vitess` | Scale testing | Distributed MySQL |

**Plugin registration:**

```go
func init() {
    storage.Register("sqlite", sqlite.New)
    storage.Register("memory", memory.New)
}
```

Config selects the backend:

```go
cfg := &Config{
    StorageBackend: "sqlite",      // or "memory", "postgres"
    DataDir:        "/tmp/dynamo",  // for sqlite
    ShardsPerTable: 4,
}
```

### 5.1 Open-Source Reuse Plan

The fastest path is to reuse protocol/model/parsing building blocks and keep `dql`-specific behavior in planner/executor code:

| Area | Library | How We Use It |
|---|---|---|
| API model types | `github.com/aws/aws-sdk-go-v2/service/dynamodb/types` | Canonical request/response shapes in handlers/tests |
| AV JSON compatibility | `github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue` | Marshal/unmarshal DynamoDB JSON (`{"S":"x"}`, etc.) |
| Storage | `modernc.org/sqlite` | Default backend with WAL + sharding |
| Binary encoding | `github.com/fxamacker/cbor/v2` | Compact item payload encoding |
| Expression parser (optional) | `github.com/alecthomas/participle/v2` | Reduce handwritten parser surface area |
| PartiQL subset parsing | `vitess.io/vitess/go/vt/sqlparser` | Parse SQL-like subset for statement translation |

Decision rule: prefer mature libraries for syntax/model handling; keep DynamoDB behavior mapping owned by `dql`.

---

## 6. Testing Strategy

### 6.0 Test-First Rule

Every API capability lands with:

- AWS SDK v2 integration coverage against `dql`
- Differential checks against DynamoDB Local for response shape/error parity (accepted Phase 0 baseline)
- Optional targeted spot checks against real AWS DynamoDB only for ambiguous edge cases
- Unit tests for parser/planner/storage edge conditions

### 6.1 Conformance Tests

- Port/adapt the **official DynamoDB local test suite** if available, or build one from the AWS SDK docs.
- Use the **AWS Go SDK v2** as a client in integration tests. Point it at `http://localhost:8000`.
- Test matrix: every API operation × edge cases (empty table, missing key, condition failures, etc.)

### 6.2 Compatibility Tests

- Run real-world DynamoDB test suites from popular Go libraries (e.g., `guregu/dynamo`, `aws/aws-sdk-go-v2`) against this server.
- Fuzz the expression parser.

### 6.3 Performance Tests

- Benchmark: parallel PutItem/GetItem throughput vs. DynamoDB Local Java.
- Target: **>100x throughput** on parallel workloads (Java DynamoDB Local is ~1000 ops/sec due to global lock; we should hit 100K+ with SQLite WAL + sharding).
- Benchmark expression parsing and evaluation.

### 6.4 Unit Tests

```
Every pkg/ directory gets a _test.go with:
  - Table-driven tests
  - Property-based tests for key encoding (roundtrip)
  - Fuzz tests for expression parser
```

### 6.5 AWS SDK v2 Compatibility Checklist

Pass criteria for each item:

- Same HTTP status class and DynamoDB error code family
- Same required response fields and data types
- Semantically equivalent behavior (excluding request IDs, timestamps, and capacity metadata when intentionally stubbed)

#### Protocol and Error Surface

- [x] `X-Amz-Target` dispatch for `DynamoDB_20120810.*` operations
- [x] `application/x-amz-json-1.0` request/response compatibility
- [x] Unknown operation returns DynamoDB-style validation error shape
- [x] Validation errors include operation-appropriate `__type`/message conventions

#### Table Lifecycle

- [x] `CreateTable` (HASH only)
- [x] `CreateTable` (HASH+RANGE)
- [x] `DescribeTable`
- [x] `ListTables` pagination basics
- [x] `DeleteTable`
- [ ] `UpdateTable` (stream toggle + basic GSI status transitions/stubs)

#### Core Item Operations

- [x] `PutItem` (insert/replace)
- [ ] `PutItem` with `ConditionExpression`
- [x] `GetItem` (hit/miss)
- [x] `DeleteItem` with `ReturnValues=ALL_OLD`
- [ ] `UpdateItem` with `SET`/`REMOVE`/`ADD`/`DELETE`
- [ ] `UpdateItem` conditional failure behavior

#### Query and Scan

- [ ] `Query` with partition key equality
- [ ] `Query` with sort key operators (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`)
- [ ] `Query` `Limit` + `LastEvaluatedKey` + `ExclusiveStartKey`
- [ ] `Scan` full table + filtered scan
- [ ] `ProjectionExpression` + `ExpressionAttributeNames`
- [ ] `Select=COUNT`

#### Expression Semantics

- [ ] `ExpressionAttributeNames` substitution correctness
- [ ] `ExpressionAttributeValues` substitution correctness
- [ ] Functions: `attribute_exists`, `attribute_not_exists`, `begins_with`, `contains`, `size`
- [ ] Logical composition: `AND`/`OR`/`NOT` with expected precedence

#### Batch and Transactions

- [ ] `BatchWriteItem` put/delete mix across tables
- [ ] `BatchGetItem` key batches across tables
- [ ] `TransactWriteItems` atomic success path
- [ ] `TransactWriteItems` rollback on condition failure
- [ ] `TransactGetItems` consistent multi-item read behavior

#### Streams, PartiQL, and Advanced

- [ ] Streams: `ListStreams`, `DescribeStream`, `GetShardIterator`, `GetRecords`
- [ ] PartiQL `ExecuteStatement` for `SELECT`/`INSERT`/`UPDATE`/`DELETE` subset
- [ ] TTL background expiration behavior (including stream emission)
- [ ] Backup/restore stubs return compatible shapes/status codes


### 6.6 Current Phase 0/1 Progress Snapshot

- Completed: compatibility harness, operation dispatch/error surface, table lifecycle (`CreateTable`/`DescribeTable`/`ListTables`/`DeleteTable`), and memory-backed `PutItem`/`GetItem`/`DeleteItem` paths validated by AWS SDK v2 integration tests.
- In progress: `UpdateItem` parity beyond legacy `AttributeUpdates` flow, conditional expressions, and query/scan coverage.

### 6.7 Differential Harness Runtime (DynamoDB Local via Testcontainers)

Phase 0 differential baseline uses DynamoDB Local (no AWS account required):

- Container image: `amazon/dynamodb-local:latest`
- Orchestration: `github.com/testcontainers/testcontainers-go`
- Comparison path: run identical AWS SDK calls against:
  - `dql` in-process `httptest` server
  - ephemeral DynamoDB Local container endpoint

Environment requirements for CI/dev machines:

- Docker Engine installed
- Docker daemon running and reachable (default Unix socket or equivalent)
- Current user allowed to access Docker daemon

If Docker is unavailable, the differential test should be marked as skipped with an explicit setup message; the remaining non-differential integration tests should still run.

Real AWS DynamoDB comparisons are optional and are not required for Phase 0 completion.

---

## 7. Project Structure

```
dynamo-go-local/
├── cmd/
│   └── dql/
│       └── main.go                  — CLI entrypoint (flags: port, data dir, etc.)
├── pkg/
│   ├── api/
│   │   ├── server.go
│   │   ├── dispatcher.go
│   │   ├── errors.go
│   │   └── types.go
│   ├── types/
│   │   ├── attributevalue.go
│   │   ├── compare.go
│   │   └── item.go
│   ├── table/
│   │   ├── catalog.go
│   │   ├── schema.go
│   │   └── validate.go
│   ├── expression/
│   │   ├── lexer.go
│   │   ├── parser.go
│   │   ├── ast.go
│   │   ├── evaluator.go
│   │   ├── projection.go
│   │   ├── update.go
│   │   ├── functions.go
│   │   └── keyeval.go
│   ├── storage/
│   │   ├── engine.go
│   │   ├── types.go
│   │   ├── registry.go
│   │   ├── sqlite/
│   │   │   ├── backend.go
│   │   │   ├── db.go
│   │   │   ├── schema.go
│   │   │   ├── keyencode.go
│   │   │   ├── query.go
│   │   │   └── shard.go
│   │   └── memory/
│   │       └── backend.go
│   ├── streams/
│   │   ├── changelog.go
│   │   ├── record.go
│   │   ├── shard.go
│   │   └── handlers.go
│   ├── partiql/
│   │   ├── lexer.go
│   │   ├── parser.go
│   │   ├── ast.go
│   │   ├── planner.go
│   │   └── executor.go
│   ├── handlers/
│   │   ├── put_item.go
│   │   ├── get_item.go
│   │   ├── delete_item.go
│   │   ├── update_item.go
│   │   ├── query.go
│   │   ├── scan.go
│   │   ├── create_table.go
│   │   ├── delete_table.go
│   │   ├── describe_table.go
│   │   ├── list_tables.go
│   │   ├── update_table.go
│   │   ├── batch_write_item.go
│   │   ├── batch_get_item.go
│   │   ├── transact_write_items.go
│   │   ├── transact_get_items.go
│   │   ├── execute_statement.go
│   │   ├── batch_execute_statement.go
│   │   └── execute_transaction.go
│   ├── concurrency/
│   │   └── striped_lock.go
│   └── ttl/
│       └── reaper.go
├── internal/
│   └── testutil/
│       ├── fixtures.go
│       └── client.go            — Pre-configured AWS SDK client for tests
├── go.mod
├── go.sum
├── Dockerfile
├── Makefile
└── README.md
```

---

## 8. Key Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/aws/aws-sdk-go-v2/service/dynamodb/types` | Canonical DynamoDB request/response model types |
| `github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue` | DynamoDB AttributeValue JSON translation helpers |
| `modernc.org/sqlite` | Pure Go SQLite backend (default) |
| `github.com/fxamacker/cbor/v2` | CBOR encoding for item storage |
| `vitess.io/vitess/go/vt/sqlparser` | PartiQL SQL-subset parsing |
| `github.com/alecthomas/participle/v2` | Optional expression parser support |
| `stretchr/testify` | Test assertions |
| Standard library | HTTP server, JSON, sync primitives |

**Philosophy:** Keep dependencies intentional, but reuse mature OSS for model/syntax handling to reduce implementation risk and speed up delivery.

---

## 9. Configuration & CLI

```bash
# Start the server
dql serve \
  --port 8000 \
  --data-dir /tmp/dql-data \
  --storage sqlite \
  --shards-per-table 4 \
  --in-memory          # SQLite :memory: mode for ephemeral test runs

# Or as a Go test helper (programmatic)
srv := dql.NewServer(dql.WithInMemory(), dql.WithPort(0)) // random port
defer srv.Close()
endpoint := srv.Endpoint() // "http://127.0.0.1:54321"
```

The **programmatic API** is critical — teams should be able to spin up a server in `TestMain` or per-test with zero friction.

---

## 10. Milestone Timeline (Rough Estimate)

| Phase | Scope | Duration | Deliverable |
|---|---|---|---|
| **0** | Compatibility harness + differential tests | 2-3 days | AWS SDK v2 tests can run against DynamoDB Local and `dql` |
| **1** | HTTP dispatch + table lifecycle + memory backend CRUD | 1-2 weeks | Core operations pass checklist on in-memory backend |
| **2** | SQLite backend + pagination + index basics | 1-2 weeks | Durable backend passes same CRUD/query checklist |
| **3** | Expression engine | 1-2 weeks | Condition/filter/projection/update expressions pass compatibility cases |
| **4** | Batch + Transactions | 1 week | BatchWrite/Get and TransactWrite/Get parity for covered cases |
| **5** | Streams | 1-2 weeks | Streams API + change capture pass integration checks |
| **6** | PartiQL subset | 1 week | ExecuteStatement subset mapped to existing operations |
| **7** | Advanced + polish | 2-3 weeks | TTL, backups/stubs, conformance expansion, benchmarks |

**Total: ~8-12 weeks** to a production-quality tool with earlier working milestones.

---

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| DynamoDB expression grammar is complex and underdocumented | Parser bugs, incompatibilities | Fuzz testing, test against DynamoDB Local for edge cases, and use real AWS spot checks only when behavior is ambiguous |
| SQLite WAL write contention under heavy parallel writes | Slower than expected | Partition sharding (N SQLite files per table), or use memory backend for tests |
| `modernc.org/sqlite` performance vs CGo | May be 2-3x slower | Offer build tag for `mattn/go-sqlite3` CGo backend |
| PartiQL subset differences from SQL parsers | Missing/incorrect edge cases | Start with explicit supported subset, differential tests, then expand |
| Number type precision (DynamoDB uses 38-digit decimal) | Rounding errors | Use `cockroachdb/apd` or similar arbitrary-precision library  |

---

## 12. Future Extensions

- **Docker image** — `docker run -p 8000:8000 dql` as a drop-in replacement
- **Testcontainers integration** — Go module for easy test setup
- **Lambda trigger simulation** — invoke a local Lambda on stream events
- **Metrics / observability** — Prometheus endpoint for ops/sec, latency histograms
- **CBOR wire protocol** — DynamoDB is moving toward CBOR; support both JSON and CBOR on the wire
- **Snapshot/restore** — dump/load table data for test fixture management
