# PartiQL `ExecuteStatement` Support Plan

## Objective

Add DynamoDB-compatible support for `ExecuteStatement` with a pragmatic first slice that maps PartiQL statements onto already-implemented table/item/query/scan capabilities.

## Research Summary (Current Codebase)

### What already exists and can be reused

1. **HTTP operation dispatch is centralized** in `pkg/api/server.go` and already routes DynamoDB targets to operation handlers.
2. **Core storage primitives are implemented** in `pkg/storage/engine.go` (`PutItem`, `GetItem`, `UpdateItemConditional`, `DeleteItem`, `Query`, `Scan`).
3. **Expression parsing/evaluation support exists** in `pkg/api/expressions.go`, including condition/filter parsing and update-expression behavior.
4. **Integration test harness is in place** and already validates end-to-end SDK behavior against the local server in `integration/phase0_phase1_test.go`.
5. The project plan explicitly marks `ExecuteStatement` as a pending Streams/PartiQL item in `README.md`.

### Key constraints to account for

- No dedicated PartiQL package exists yet.
- API currently does not route `ExecuteStatement`.
- Existing engine API has no direct “SQL layer,” so translation/planning is required rather than direct execution.

## Scope

### Phase A (MVP)

Support `ExecuteStatement` for a constrained subset:

- `SELECT ... FROM <table> WHERE pk = ? [AND sk predicate]`
- `INSERT INTO <table> VALUE {...}`
- `UPDATE <table> SET ... WHERE pk = ? [AND sk = ?]`
- `DELETE FROM <table> WHERE pk = ? [AND sk = ?]`

With:

- Positional parameters via `Parameters` (DynamoDB AttributeValues)
- `ConsistentRead` pass-through where meaningful
- Response shapes compatible with DynamoDB SDK expectations (`Items`, `NextToken`, etc.)

### Explicitly out of scope for Phase A

- `BatchExecuteStatement`
- Complex joins/subqueries
- Rich path/list mutations beyond what current update engine supports
- Full PartiQL grammar parity

## Design Approach

## 1) API Surface: Add handler and routing

**Files**: `pkg/api/server.go` (+ new helper file under `pkg/api/`)

- Add target dispatch case for `ExecuteStatement`.
- Parse DynamoDB `ExecuteStatementInput` JSON payload.
- Return DynamoDB-compatible error envelopes using existing `writeError`/typed error flow.

Deliverable:

- `executeStatement` handler skeleton with validation and plumbing to planner/executor.

## 2) New PartiQL package with “parse → plan → execute”

**New package**: `pkg/partiql/`

Recommended files:

- `ast.go`: minimal internal AST for supported statement classes.
- `parser.go`: constrained parser for supported syntax.
- `bind.go`: positional parameter binding (`?`) to typed AttributeValues.
- `planner.go`: convert AST + bound params into internal execution plans.
- `executor.go`: run plans via existing `storage.Engine` and return API-ready results.
- `errors.go`: PartiQL-specific validation errors normalized to DynamoDB-style messages.

Rationale:

- Keeps SQL-ish concerns separate from request marshalling.
- Enables incremental grammar expansion without destabilizing core handlers.

## 3) Translation strategy by statement type

### SELECT

- Parse table name + predicate.
- If predicate is key-compatible (`pk` equality plus optional supported sort-key condition), route to `Engine.Query`.
- Otherwise, route to `Engine.Scan` + filter fallback (for subset expressions we can evaluate).
- Map projection list to returned attributes where feasible.

### INSERT

- Parse VALUE object into AttributeValue map.
- Call `Engine.PutItem`.
- Return empty `Items` unless DynamoDB behavior requires otherwise for this operation shape.

### UPDATE

- Parse assignment list into an internal update plan.
- Convert into existing update-expression mutation flow (reuse update logic in API layer where practical).
- Execute via `UpdateItemConditional` with key-resolving predicates.

### DELETE

- Resolve key from WHERE predicate.
- Execute via `DeleteItem` / conditional variant.

## 4) Pagination and token handling

- Respect `Limit` for `SELECT`.
- Encode/decode `NextToken` as an opaque serialized `LastEvaluatedKey` payload.
- Keep token format internal and versioned (e.g., JSON with statement hash + key map) to avoid future incompatibility.

## 5) Error compatibility strategy

Normalize parser/planner failures into DynamoDB-like `ValidationException` errors:

- Unsupported syntax
- Missing key predicates for key-required operations
- Parameter count/type mismatches
- Invalid identifiers/paths

Avoid leaking internal parser errors directly to clients.

## Implementation Milestones

## Milestone 1: Plumbing and no-op guardrails

- Add dispatch for `ExecuteStatement`.
- Add handler that returns clear “unsupported statement” validation errors.
- Add integration test asserting non-implemented statement surfaces correct error shape.

## Milestone 2: Read path (`SELECT`)

- Implement parser subset for SELECT.
- Implement query-vs-scan planner decision.
- Add pagination (`Limit`, `NextToken`) and basic projection support.
- Add integration tests for:
  - partition-key equality select
  - sort-key predicates
  - parameter binding
  - next-token pagination

## Milestone 3: Write path (`INSERT`/`UPDATE`/`DELETE`)

- Implement INSERT VALUE parsing and execution.
- Implement UPDATE SET subset with key predicates.
- Implement DELETE with key predicates.
- Add integration tests for happy path and validation failures.

## Milestone 4: Differential parity checks

- Extend `integration/differential_test.go` with ExecuteStatement cases against DynamoDB Local.
- Document known divergences and gate unsupported constructs with explicit validation errors.

## Testing Plan

## Unit tests

- `pkg/partiql/parser_test.go`: grammar coverage for supported subset and malformed statements.
- `pkg/partiql/bind_test.go`: parameter count/type/order validation.
- `pkg/partiql/planner_test.go`: key-condition classification and operation routing.

## Integration tests

- Add `ExecuteStatement` tests to `integration/phase0_phase1_test.go` (or split into `integration/partiql_test.go`).
- Validate SDK-level response shapes and error types.

## Differential tests

- Add baseline parity tests in `integration/differential_test.go` for supported statements only.
- Skip/xfail unsupported syntax with explicit reason.

## Risks and Mitigations

1. **Grammar creep**: PartiQL is broad.
   - Mitigation: enforce explicit supported subset and fail closed.

2. **Error-message parity drift** with DynamoDB Local.
   - Mitigation: differential tests and normalized validation-error helper.

3. **Update semantics mismatch** between PartiQL assignments and current UpdateExpression engine.
   - Mitigation: introduce a narrow assignment subset first, backed by deterministic unit tests.

4. **Token compatibility churn** for pagination.
   - Mitigation: versioned opaque token encoding.

## Proposed Task Breakdown (Execution Order)

1. Add API route + stub handler for `ExecuteStatement`.
2. Introduce `pkg/partiql` scaffolding (`ast`, `errors`, `executor` interfaces).
3. Implement SELECT parser/planner/executor with parameters and pagination.
4. Implement INSERT executor path.
5. Implement UPDATE and DELETE executor paths.
6. Add/expand unit tests for parser/planner/binder.
7. Add integration + differential coverage.
8. Document supported syntax and known limits in `README.md`.

## Definition of Done

- `ExecuteStatement` is routed and functional for the documented subset.
- Tests cover parser, planner, execution, and parity-sensitive behavior.
- Differential tests pass for supported cases against DynamoDB Local.
- Unsupported syntax returns stable, explicit `ValidationException` responses.
- README clearly documents supported PartiQL subset and gaps.
