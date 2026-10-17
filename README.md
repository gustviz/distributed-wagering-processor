# Distributed Wagering Processor

A wallet service for online casinos. It processes wagering operations (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) from game providers via **HTTP** and **AWS SQS**. It stays correct when messages are **duplicated**, arrive **out of order**, or are processed **concurrently by several instances**.

Stack: **Bun 1.3 · TypeScript (strict) · NestJS 11 · PostgreSQL · MikroORM 6 · AWS SQS (LocalStack/MiniStack) · Docker Compose**

- **Design decisions, trade-offs and limitations:** [ARCHITECTURE.md](ARCHITECTURE.md)
- **The original challenge statement:** [docs/CHALLENGE.md](docs/CHALLENGE.md)

---

## Guarantees at a glance

| Guarantee | How it is enforced |
|---|---|
| No `number` for money | `Money` holds integer minor units in a `bigint`. The database column is `numeric(20,2)`. Amounts travel as decimal strings. |
| No negative balance | `CHECK (balance >= 0)`, plus a guarded debit under a row lock |
| No lost update | `SELECT … FOR UPDATE` on the wallet row, plus a `WHERE version = ?` guard |
| No duplicate credit or debit | Unique `idempotency_key`; unique `(provider_id, external_transaction_id)`; a partial unique index allowing one reversal per transaction; `UNIQUE (transaction_id, wallet_id)` on the ledger |
| Balance matches the ledger | Deferred constraint triggers check both directions at `COMMIT` |
| Gap-free ledger | A trigger requires consecutive per-wallet versions (`previous + 1`) and `balance_before = previous balance_after` |
| Ledger is immutable | Triggers reject `UPDATE`, `DELETE` and `TRUNCATE` |
| Idempotency is persistent, never in memory | The transactions table plus the inbox `(consumer_name, message_id)` |
| No event before commit | Transactional outbox, published by a `FOR UPDATE SKIP LOCKED` relay |
| Correct with N instances | Covered by tests with 3 real OS processes, crashes and restarts |

## Quick start (Docker)

```bash
docker compose up -d --build
# postgres (host :5433) · sqs emulator (:4566) · migrate · queues · 3 × app · nginx on :3000
curl localhost:3000/health/ready
bun run smoke                                # end-to-end check of the main flows (PASS/FAIL per step)
```

> **About LocalStack:** since March 2026 the latest LocalStack images require an auth token, so `docker-compose.yml` pins `localstack/localstack:4.14.0`, the last tokenless release. To use MiniStack instead:
> `SQS_EMULATOR_IMAGE=ministackorg/ministack:1.5.27 docker compose up -d`

## Running locally (Bun)

```bash
bun install
docker compose up -d postgres sqs queues      # infrastructure only
cp .env.example .env                          # defaults point at the compose ports
bun run migrate:up                            # versioned and reversible (migrate:down)
bun run start                                 # API + SQS consumer + outbox relay + pending-reference worker
```

One image can run any combination of roles. Toggle them with `HTTP_ENABLED`, `CONSUMER_ENABLED`, `OUTBOX_RELAY_ENABLED` and `PENDING_REFERENCE_WORKER_ENABLED`.

## Tests

The integration and concurrency suites use **real PostgreSQL and a real SQS emulator**; nothing is mocked. Each test run drops and recreates a database called `wagering_test`, and every suite creates its own isolated queues.

```bash
docker compose up -d postgres sqs
bun run test:unit           # no infrastructure needed
bun run test:integration    # schema/constraints, atomicity, inbox, outbox, SQS consumer, HTTP contract
bun run test:concurrency    # races, 3 OS processes, crash after commit/before ack, kill and restart
bun run test:all            # everything (~40 s)
bun run typecheck
```

`TEST_DATABASE_URL` (default `postgres://postgres:postgres@localhost:5433/wagering_test`) and `AWS_ENDPOINT_URL` (default `http://localhost:4566`) override the targets.

| Required scenario | Where |
|---|---|
| `Money` operations and validation | `test/unit/money.test.ts` |
| `Wallet` invariants; `BET`/`WIN`/`LOSS`/`REFUND`/`ROLLBACK` rules; currency conflict; divergent payload | `test/unit/wallet.test.ts`, `test/unit/wagering-rules.test.ts`, `test/unit/domain-model.test.ts` |
| Migrations and constraints | `test/integration/schema.test.ts` |
| Atomicity of wallet, ledger, inbox and outbox; inbox and redelivery; concurrent publishers | `test/integration/transactional-boundaries.test.ts` |
| Retry, DLQ, graceful shutdown (including the grace period running out) | `test/integration/sqs-consumer.test.ts` |
| Pending-reference worker: a `FAILED` reference wakes its dependents | `test/integration/pending-references.test.ts` |
| Same BET 50× in parallel; 100 vs 2×80; parallel wallets; early ROLLBACK/REFUND | `test/concurrency/same-process.test.ts` |
| ≥ 3 instances; crash after commit/before ack; two publishers with one killed; restart and consistency | `test/concurrency/multi-instance.test.ts` |

Every concurrency and integration test ends with `assertLedgerInvariant`. Using exact `Money` arithmetic, it asserts `wallet.balance == balance rebuilt from the ledger`, consecutive versions (`1, 2, 3, …`, or from `2` for a wallet opened empty), and a correct `balance_before → balance_after` chain.

## Load test

```bash
docker compose up -d --build
TARGETS=http://localhost:3000 DURATION_S=30 CONCURRENCY=64 bun run test:load
```

The run reports throughput, p50/p95/p99, error rate, lock conflicts, retries, publish lag, and delivery. It **exits 1** if any event of the run is still unpublished after 60 s, if any event id of the run never reaches the simulated downstream consumer, if any wallet fails reconciliation, or if any request fails with a network error or HTTP 5xx. It reads the run's event ids from PostgreSQL (`LOAD_DATABASE_URL`, default: the Compose database on port 5433). The methodology and an analysis of a local run are in [ARCHITECTURE.md §15](ARCHITECTURE.md#15-load-test).

## API

| Method & path | Purpose |
|---|---|
| `POST /wallets` | Create a wallet. A positive initial balance creates an `OPENING` transaction and a ledger `CREDIT` in the same SQL transaction. |
| `GET /wallets/:walletId` | Read a wallet |
| `GET /wallets/:walletId/ledger?cursor=&limit=50` | Read the ledger, newest first, with a stable opaque cursor (max `limit` 200) |
| `POST /wallets/:walletId/reconciliation` | Compare the stored balance with the ledger. Mismatches are logged, counted in a metric and returned, never auto-fixed. |
| `POST /wagering/transactions` (`Idempotency-Key` header required) | Submit an operation |
| `GET /wagering/transactions/:transactionId` | Read a transaction by internal id |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | Read a transaction by provider id |
| `GET /health/live`, `GET /health/ready` | Liveness; readiness (checks PostgreSQL and SQS). No authentication. |
| `GET /metrics` | Prometheus metrics |

**Status codes**, identical for first calls and replays:

| Code | Meaning | Resend? |
|---|---|---|
| `200` | `PROCESSED` | Only to get the same answer back |
| `202` | `PENDING_REFERENCE`: accepted, waiting for the referenced transaction | Same key; returns the current state |
| `422` | `REJECTED` by a business rule (`failureCode` in the body) | No |
| `400` | Invalid payload (`INVALID_PAYLOAD`, `INVALID_MONEY`, …) | After fixing the payload |
| `404` | Unknown wallet or transaction (on SQS: dead-lettered, kept for audit and replay) | No |
| `409` | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT` or `WALLET_ALREADY_EXISTS` | No |
| `503` | Transient infrastructure failure (`retryable: true`, `Retry-After`) | Yes, with the same key |
| `500` | Unexpected error / `FAILED` transaction | Same key; contact the operator |

```bash
curl -X POST localhost:3000/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"p-1",
       "walletId":"<uuid>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
# {"transactionId":"…","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

SQS messages use the envelope from the challenge (`messageId`, `type: "WagerTransactionRequested"`, `occurredAt`, `data{… idempotencyKey}`) on `wager-transactions.fifo`. Use `MessageGroupId = walletId`. Integration events are published to `wager-events.fifo`.

## Project layout

```
src/
  domain/          Money, Wallet, WagerTransaction, WalletLedgerEntry, Inbox/Outbox, IntegrationEvent (no framework imports)
  application/     use cases, ports (repositories, unit of work, publisher, metrics…), idempotency hashing
  infrastructure/  MikroORM unit of work + SQL data mappers, migrations, SQS client/publisher, logger, metrics
  interface/       HTTP controllers, SQS consumer, background workers, auth seam, error classification
  app.module.ts    composition root (ports → adapters, lifecycle of the background roles)
test/  unit · integration · concurrency · load · support
```
