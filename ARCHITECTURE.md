# Architecture & Decisions

This document explains **why** the service is built the way it is. For setup and commands, see [README.md](README.md).

Contents:

1. Overview
2. Layers & boundaries
3. Domain model
4. Business rules & interpretations
5. Failure codes
6. HTTP status mapping
7. Persistence, ORM & schema
8. Concurrency
9. Idempotency
10. SQS consumption
11. Transactional outbox
12. Out-of-order references
13. Observability & reconciliation
14. Authentication
15. Load test
16. Testing strategy
17. Trade-offs, limitations & next steps

---

## 1. Overview

```
                 HTTP (provider)                         SQS wager-transactions.fifo
                       │                                            │
             ┌─────────▼─────────┐                       ┌──────────▼──────────┐
             │ WageringController│                       │ WagerTransactions-  │
             │  (zod contracts)  │                       │ Consumer (inbox)    │
             └─────────┬─────────┘                       └──────────┬──────────┘
                       └───────────────┐        ┌───────────────────┘
                                ┌──────▼────────▼──────┐
                                │ SubmitWagerTransaction│  ◄── same use case for both
                                │       use case        │
                                └──────────┬────────────┘
                     one SQL transaction   │  (wallet row locked FOR UPDATE)
     ┌─────────────┬──────────────┬────────┴─────┬───────────────┬───────────────┐
     ▼             ▼              ▼              ▼               ▼               ▼
 inbox_messages  wager_transactions  wallets  wallet_ledger_entries  outbox_messages
                                                                      │
                    PendingReferences worker ──(retries)──┘           │ FOR UPDATE SKIP LOCKED
                                                         Outbox relay ▼
                                                        SQS wager-events.fifo
```

Every instance runs the same image. It can serve HTTP, consume SQS, relay the outbox and retry pending references, or any subset of those roles. Correctness never depends on how many instances run or which roles they have: **PostgreSQL is the only arbiter**.

## 2. Layers & boundaries

| Layer | Contents | Depends on |
|---|---|---|
| `domain/` | `Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, `InboxMessage`, `OutboxMessage`, `IntegrationEvent` and its subclasses, domain errors | nothing (no NestJS, no ORM) |
| `application/` | Use cases, `WagerTransactionProcessor`, ports (`UnitOfWork`, repositories, `EventPublisher`, `Metrics`, `Clock`, `IdGenerator`, `FaultInjector`), payload hashing | domain |
| `infrastructure/` | MikroORM unit of work and SQL data mappers, migrations, SQS client/publisher, JSON logger, Prometheus | application ports |
| `interface/` | HTTP controllers, SQS consumer, periodic workers, auth seam, error classification | application |

- **Constructors and factories.** Aggregates have `private` constructors and static factories (`create`/`open`/`from`/`enqueue`). `rehydrate` rebuilds persisted state **without** re-validating transitions.
- **No ORM decorators in the domain.** Domain classes carry no ORM decorators and are never managed by the ORM.
- **Use cases are plain classes.** Nest only wires them through factory providers in `app.module.ts`, so they are unit-tested without Nest.
- **Composition root.** `app.module.ts` is the only place that knows about concrete adapters.

## 3. Domain model

**Money**
- **Representation.** An immutable `bigint` count of minor units plus an ISO-4217 code. The scale is fixed at 2, as the contract requires.
- **Parsing.** The parser accepts only plain decimals with at most 2 fraction digits. It rejects `NaN`, `Infinity`, scientific notation, empty strings, `"1."`, `"01.00"` and `"+1"`.
- **No rounding, ever.** `"10.005"` is **rejected**, not rounded.
- **Signs.** `Money.from` refuses negative amounts because no inbound contract may carry one. `Money.fromSigned` exists for internal values such as a reconciliation difference.
- **Currencies.** Arithmetic across currencies throws `CurrencyMismatchError`.
- **Range.** Values are bounded to the range of `numeric(20,2)`, so the domain never produces an amount the database would refuse.

**Wallet** (aggregate root)
- **Movements.** `debit`/`credit` return the `WalletLedgerEntry` that describes the movement. A balance change without a ledger entry cannot be expressed in the model.
- **Version.** `version` starts at 1 and is incremented **only** by a movement.
- **Opening balance.** `Wallet.open` returns `{ wallet, openingEntry? }`. A positive opening balance is a `CREDIT` at version 1, owned by an internal `OPENING` transaction.

**WagerTransaction state machine**

```
PENDING ───────────► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ─► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (another retry)
PROCESSED, REJECTED, FAILED: terminal. Any transition throws InvalidTransactionStateError (a programming error).
```

- **`PENDING` is never persisted.** Processing is synchronous inside one SQL transaction, so `PENDING` exists only in memory between `create()` and the decision. The schema still accepts it, so an asynchronous intake could be added later.
- **The database enforces terminality too.** A trigger refuses any `UPDATE` of a terminal row, and of the business columns of any row.

**WalletLedgerEntry.** Frozen and append-only. `create` checks `balanceBefore ± money === balanceAfter`. `walletVersion` forms a dense chain per wallet, which is also the pagination key.

**IntegrationEvent.** An abstract envelope with one subclass per event (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`). `eventType` and `version` live on the class. `data` holds only JSON (`MoneyProps`, never `Money`).
- **`aggregateId` is the walletId for every event.** The wallet is the consistency boundary and the ordering key. The transaction id travels in `data`.

## 4. Business rules & interpretations

| Operation | Balance | Ledger | Notes |
|---|---|---|---|
| `BET` | debit | 1 `DEBIT` | `INSUFFICIENT_FUNDS` if `balance < amount` |
| `WIN` | credit | 1 `CREDIT` | May reference a `BET`; the reference is optional |
| `LOSS` | – | none | `PROCESSED` and emits `WagerTransactionProcessed`; the amount may be `0.00` |
| `REFUND` | credit | 1 `CREDIT` | Requires a `PROCESSED` `BET`; amount must be equal |
| `ROLLBACK` | inverse of reference | 1 inverted entry | Reference must be a `BET`, `WIN` or `REFUND`; amount must be equal |

Interpretations I adopted:

1. **A transaction is reversed at most once, by any kind.** The challenge forbids reversing twice "by the same type". I made it stricter: a `BET` that was refunded cannot also be rolled back, and the reverse also holds. Otherwise the stake would be credited twice. A partial unique index enforces this (`uq_wager_tx_single_reversal`).
2. **References are resolved within the same provider** by `(providerId, referenceExternalTransactionId)`. They must match on player, wallet, currency and round; otherwise the result is `REFERENCE_MISMATCH`.
3. **Order of reference checks:** ownership (mismatch), then kind, then the reference's own state, then amount, then "already reversed".
   - A reference that is still `PENDING_REFERENCE` makes the dependent **wait** too.
   - A reference that is `REJECTED` or `FAILED` gives `REFERENCE_NOT_PROCESSED`.
4. **`WIN`/`LOSS` with a reference** follow the same rules: the reference must be a `BET` that is `PROCESSED`, in the same round.
   - The reference is a link for auditing, not a settlement lock. A `WIN`/`LOSS` may reference a `BET` that was already refunded or rolled back, and several `WIN`s may reference the same `BET` (multi-payout games). The provider owns the round's outcome; the wallet only guarantees that each operation is applied once and that the balance never goes negative.
5. **Amounts.** `BET`, `WIN`, `REFUND` and `ROLLBACK` need an amount `> 0`. `LOSS` accepts `>= 0`.
6. **Currency or player mismatch** with the wallet is a persisted `REJECTED` (`CURRENCY_MISMATCH`, `PLAYER_WALLET_MISMATCH`). It stays auditable and replays return the same rejection.
7. **Unknown wallet: `404 WALLET_NOT_FOUND`, not persisted.** The transaction row has a foreign key to its wallet, so there is nothing to store a rejection against. Over HTTP the provider gets the 404 directly. Over SQS the message goes to the DLQ instead of being acked, so the operation is not silently lost (§10).
8. **A reversal that cannot be paid** (a `ROLLBACK` of a `WIN` or `REFUND` with insufficient balance) is **rejected explicitly** with `REVERSAL_INSUFFICIENT_FUNDS`. That code differs from a `BET`'s `INSUFFICIENT_FUNDS`, because the money was already paid out and must be recovered out-of-band. Negative balances are never allowed.
9. **`OPENING` is internal.** The contracts don't accept it, and the provider id `internal` is reserved.
10. **Replays return the original outcome**: same `transactionId`, status, `failureCode`, and the balance **observed when the decision was made**, which is stored in `observed_balance`.
    - For `PENDING_REFERENCE`, a replay returns the transaction's *current* state. If it was processed in the meantime, the provider sees `PROCESSED`.

## 5. Failure codes

Every code is stable and machine-readable. A rejection is final for that idempotency key: resending the same key always gets the same answer.

| Code | When | Provider action |
|---|---|---|
| `INSUFFICIENT_FUNDS` | `BET` > balance | Give up (player cannot afford it) |
| `REVERSAL_INSUFFICIENT_FUNDS` | A `ROLLBACK` debit would make the balance negative | Operational follow-up; do not resend |
| `CURRENCY_MISMATCH` | Operation currency ≠ wallet currency | Fix and send under a **new** external id |
| `PLAYER_WALLET_MISMATCH` | `playerId` doesn't own `walletId` | Fix and send under a new external id |
| `REFERENCE_NOT_FOUND` | Reference didn't arrive within the retry budget (§12) | Send the reference, then a new reversal |
| `REFERENCE_MISMATCH` | Reference has a different player, wallet, currency or round | Fix the reference |
| `REFERENCE_KIND_NOT_ALLOWED` | e.g. `REFUND` → `WIN` | Fix the reference |
| `REFERENCE_NOT_PROCESSED` | Reference is `REJECTED`/`FAILED`, or still pending when the budget ran out | Give up |
| `REFERENCE_ALREADY_REVERSED` | Reference already refunded or rolled back | Give up (already done) |
| `REVERSAL_AMOUNT_MISMATCH` | Partial reversal (out of scope) | Fix the amount |
| `UNEXPECTED_ERROR` (`FAILED`) | Permanent non-business failure in the worker; kept for audit | Contact the operator |

## 6. HTTP status mapping

There is one classifier (`interface/error-classification.ts`) for both transports, so the HTTP status and the SQS disposition can never disagree.

| Situation | HTTP | SQS |
|---|---|---|
| Processed (new or replay) | 200 | ack |
| Accepted, pending reference | 202 | ack |
| Business rejection | 422 (+ `failureCode`) | ack |
| Invalid payload (contract, `Money`) | 400 | DLQ |
| Idempotency or external-id conflict | 409 | DLQ |
| Unknown wallet or transaction | 404 | DLQ (`not_found`: kept for audit and replay, see §10) |
| Transient infrastructure failure | 503 + `Retry-After` | retry with backoff, then DLQ |
| Unexpected error | 500 | retry with backoff, then DLQ |

Errors share one body format: `{"error":{"code","message","retryable","details?"}}`. A provider can decide whether to resend from the status and `retryable` alone.

## 7. Persistence, ORM & schema

### ORM choice: MikroORM 6, used as a transaction and migration engine

MikroORM provides:
- connection pooling;
- **transaction demarcation** with an explicit isolation level (`em.transactional`, on a fresh `em.fork()` per unit of work, so no identity map is shared between requests);
- **versioned, reversible migrations** (`Migration.up/down`, run by the `Migrator`).

The repositories are **data mappers that issue explicit SQL through the transaction-bound `EntityManager`**. I chose that over entity mapping plus `flush()` for three reasons:

- **The critical statements need exact semantics:** `SELECT … FOR UPDATE`, `FOR UPDATE SKIP LOCKED`, `INSERT … ON CONFLICT DO NOTHING RETURNING`, and `UPDATE … WHERE version = ?` with an affected-row check. Reviewing them as SQL beats reverse-engineering what a flush will emit.
- **The domain stays persistence-ignorant.** Private constructors, `rehydrate` factories and no decorators would fight entity hydration.
- **An identity map is a liability inside retry loops.** A cached entity can hide a row that another instance has just committed. Here every read hits the database, under the lock that protects it.

v6 was pinned deliberately: v7 had just been released and v6 is the line I can vouch for. MikroORM was preferred over TypeORM because its unit of work and `transactional()` API are explicit.

### Money mapping

Each amount is stored as `numeric(20,2)` plus a separate `currency text CHECK (~ '^[A-Z]{3}$')`. PostgreSQL returns numerics as **strings**, and the mappers build `Money` from those strings directly, so no value ever passes through a JS `number`. `numeric(20,2)` allows up to 18 integer digits, and `Money` enforces the same bound.

### Schema-enforced invariants (migrations `…0001`, `…0002`, `…0003`)

Migration `…0003` tightened the ledger insert trigger from "increasing" to "consecutive" versions. Before it, a transaction that moved a wallet twice but wrote a ledger entry only for the second move passed every check. It is a new migration rather than an edit of `…0002`, so databases that already ran `…0002` get the fix through `migrate up`, and `down` restores the previous function.

| Guarantee | Mechanism |
|---|---|
| One wallet per player + currency | `uq_wallets_player_currency` |
| Non-negative balance | `ck_wallets_balance_non_negative`, `ck_ledger_balances_non_negative` |
| Idempotency | `uq_wager_tx_idempotency_key`, `uq_wager_tx_provider_external` |
| One reversal per transaction | Partial unique index `uq_wager_tx_single_reversal … WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'` |
| At most one entry per transaction and wallet | `uq_ledger_transaction_wallet` |
| Ledger arithmetic | `ck_ledger_arithmetic` (`after = before ± amount`) |
| Gap-free version chain | `uq_ledger_wallet_version`, plus trigger `guard_ledger_insert` (migration `…0003`): each entry is exactly `previous version + 1` and `balance_before = previous balance_after`. A wallet's first entry is version 1 only for the OPENING credit of a funded wallet; a wallet opened empty is version 1 with no entry, so its first movement is version 2. An OPENING can only be the first entry. |
| Ledger is append-only | Triggers on `UPDATE`/`DELETE` (row level) and `TRUNCATE` (statement level) |
| Wallets and transactions are never deleted | Triggers |
| Terminal transactions and business columns are immutable | Trigger `guard_wager_transaction_update` |
| `version` moves +1 **iff** `balance` moves | Trigger `guard_wallet_update` |
| A ledger entry ⇒ a `PROCESSED`, balance-moving transaction with the same wallet, amount and currency, and the right direction | Trigger `guard_ledger_insert` |
| **Balance change ⇔ ledger entry** | `DEFERRABLE INITIALLY DEFERRED` constraint triggers, checked at `COMMIT` in both directions |
| Status and field coherence | `CHECK`s: failure code iff `REJECTED`/`FAILED`; `processed_at` iff `PROCESSED`; `next_attempt_at` iff `PENDING_REFERENCE`; reference required for reversals; `OPENING` iff provider `internal`, … |

Indexes:
- `ix_wager_tx_pending_due` (partial, `PENDING_REFERENCE`) for the worker;
- `ix_wager_tx_pending_dependents` to wake dependents;
- `ix_outbox_unpublished` (partial) for the relay;
- unique `(wallet_id, wallet_version)` for keyset pagination and the chain lookup.

`test/integration/schema.test.ts` attacks each of these guarantees with raw SQL that bypasses the application.

## 8. Concurrency

**The unit of concurrency is the wallet row.** Each submission runs in one `READ COMMITTED` transaction:

1. Inbox insert (SQS only), with `ON CONFLICT DO NOTHING`.
2. Idempotency lookup without a lock: a cheap replay path.
3. `SELECT … FROM wallets WHERE id = ? FOR UPDATE`. This serializes all writers of that wallet, and **only** that wallet.
4. Idempotency lookup **again**, under the lock. A duplicate that waited now sees the committed original and replays it.
5. Domain decision in memory, then writes in foreign-key order: transaction → `UPDATE wallets … WHERE id = ? AND version = ?` (affected rows must be 1) → ledger entry → outbox → wake dependents.

Why **pessimistic row lock + version guard + unique constraints**:

- **Optimistic locking alone fails on a hot wallet.** Retry storms grow with contention, and every retry re-runs the reference lookup. A row lock turns contention into a short queue, so lock conflicts were 0 under load (§15).
- **The version guard is defence in depth.** If any path ever wrote without the lock, the update would hit 0 rows and raise `ConcurrencyConflictError`, which is retryable.
- **Unique constraints are the final arbiter.** Two duplicates can lock *different* wallets: a conflicting payload can name another wallet. Then the loser gets a unique violation on the idempotency key, and the retry turns it into a replay or a conflict.

**Deadlock freedom.** A submission locks exactly one wallet. Every path that touches existing transaction rows takes the **wallet lock first**, then the transaction row. That covers `ResolvePendingReferences`, `markFailed`, and the claim, which touches transaction rows only. `lock_timeout` (5 s by default) bounds waits, and timeouts or deadlocks surface as retryable contention.

**Retries.** Contention and unique-violation races are retried up to 4 times with jitter inside the use case. Database outages are not retried there: they map to 503 or an SQS retry, so the caller backs off instead of hammering the database. Connection attempts are bounded (3 s to connect, 5 s to get a pooled connection). Without those bounds, every request hung about 30 s while PostgreSQL was down, roughly the SQS visibility timeout.

**Instance count is irrelevant.** Every guarantee lives in PostgreSQL. The test suite runs 3 separate OS processes, each with its own pool, consumer, relay and worker.

## 9. Idempotency

- **Key.** The `Idempotency-Key` header (`data.idempotencyKey` on SQS) is the source of truth; providers are recommended to use `{providerId}:{externalTransactionId}`. Keys are stored with a unique constraint, never cached in memory.
- **`payloadHash`** = `hex(SHA-256(canonicalJSON(businessFields)))`.
  - **Business fields:** `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money{amount, currency}`, `referenceExternalTransactionId` (when present).
  - **Canonical JSON:** keys sorted lexicographically (recursively), no whitespace, `undefined` omitted.
  - **The amount is normalized first** (`"25.5"` → `"25.50"`), so equal amounts hash equally.
  - **Excluded:** the header, the SQS envelope (`messageId`, `occurredAt`, `type`) and every transport field.
- **Same key and same hash:** the original result is returned with `idempotentReplay: true`, and the HTTP status is the same as the first time.
- **Same key, different hash:** `409 IDEMPOTENCY_KEY_CONFLICT`. It is never a replay.
- **Same `(provider, externalTransactionId)` under a different key:** `409 EXTERNAL_TRANSACTION_CONFLICT`. The provider's transaction id is already taken.
- **Inbox, `(consumer_name, message_id)`.**
  - **Same transaction as the effects.** The row is written in the same SQL transaction as the effects, as its first statement. A concurrent redelivery blocks on the unique key until the first transaction commits, then sees the row as a duplicate.
  - **Different body under the same `messageId`:** that is a producer bug, so the message goes to the DLQ (`INBOX_PAYLOAD_CONFLICT`).
  - **New `messageId` for an already-applied operation:** this is answered by the idempotency key, and the inbox row is still recorded.

## 10. SQS consumption

- **Same use case as HTTP.** The consumer long-polls (`WaitTimeSeconds`) and calls `SubmitWagerTransaction`, exactly like the HTTP entry point.
- **Ack only after commit.** `DeleteMessage` is sent after the use case returns, i.e. after `COMMIT`.
  - **Crash between commit and ack:** the message is redelivered and absorbed by the inbox. Tested by killing a real process at that exact point (`FAULT_INJECTION=sqs.after-commit-before-ack`).
- **Error classes** (§6):
  - **Business outcomes** (`PROCESSED`, `REJECTED`, `PENDING_REFERENCE`): already persisted and auditable in the database, so ack.
  - **Contract violations, conflicts and unknown wallets:** permanent, so redelivery cannot help. They are sent explicitly to `wager-transactions-dlq.fifo` with `failureReason`, `errorCode` and `receiveCount` attributes, then deleted from the input queue.
    - **Why an unknown wallet is dead-lettered, not acked:** nothing can be persisted for it, because a transaction row needs an existing wallet. Acking would erase the only record that the operation ever arrived. In the DLQ (`failureReason=not_found`, `errorCode=WALLET_NOT_FOUND`) the original body is kept, so it stays auditable and can be replayed once the wallet exists or the id is corrected.
  - **Transient and unexpected errors:** `ChangeMessageVisibility(min(base·2^(receiveCount-1), max))`, i.e. exponential backoff driven by `ApproximateReceiveCount`. After `SQS_MAX_ATTEMPTS` (5) deliveries the consumer moves the message to the DLQ itself.
  - **Safety net:** the queue's redrive policy (`maxReceiveCount = 10`) also routes to the DLQ in case the consumer itself keeps crashing.
- **FIFO.** Producers use `MessageGroupId = walletId`. Within a received batch, each group is handled sequentially and different groups in parallel. If a message is not acked, the remaining messages of its group are released with the same delay, preserving order.
- **SQS FIFO deduplication is an optimization, never the guarantee.** The tests resend identical bodies with fresh deduplication ids to simulate real redeliveries.
- **`SIGTERM`.** Polling stops (the in-flight `ReceiveMessage` is aborted). Messages already being processed finish and are acked within `SHUTDOWN_GRACE_MS`. Messages received but not started are returned with visibility 0. Messages still being processed when the grace period ends are also returned with visibility 0, so another instance picks them up at once instead of after the visibility timeout. If such a message still commits on the way out, its redelivery is absorbed by the inbox. Then Nest closes HTTP and, last, the database pool. The Docker `CMD` uses exec form, so Bun is PID 1 and receives the signal.

## 11. Transactional outbox

- **Same transaction as the change.** Events are inserted in the same SQL transaction as the state change that produced them. No code path publishes before commit.
- **The relay**, `RelayOutboxUseCase`, runs on every instance:
  1. `SELECT … WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now) ORDER BY seq LIMIT 100 FOR UPDATE SKIP LOCKED`.
  2. Publish to `wager-events.fifo`, with chunks of 10 sent in parallel.
  3. Mark the rows published (one bulk `UPDATE`) or schedule a retry (capped exponential backoff with jitter).
  4. `COMMIT`.
- **Any number of relays can run.** `SKIP LOCKED` means no two relays ever hold the same row.
- **Crash after publish, before commit.** The locks are released and the rows are still unpublished, so another relay publishes them again. Delivery is **at-least-once**, and the duplicates carry the **same `eventId`**, which is also the FIFO `MessageDeduplicationId`. Consumers de-duplicate by `eventId` (or order by `walletVersion` for `WalletBalanceChanged`). The integration tests show the republished id; the multi-process test kills a real publisher at that point.
- **Trade-off: the publish happens while row locks are held.** That is what makes double publication by *live* relays impossible. The cost is a transaction held open for one SQS round-trip, bounded by the client timeout and the batch size.
- **The outbox never gives up.** It must not lose a confirmed event, so failures back off up to 60 s and show up in `wagering_outbox_lag_seconds` and `wagering_outbox_publish_failures_total`.
- **Ordering is best-effort.** Parallel relays and chunks can interleave. Each event carries `walletVersion` and `occurredAt` so consumers can order.

## 12. Out-of-order references

- **Waiting.** A transaction whose reference is unknown (or still pending) is persisted as `PENDING_REFERENCE` with `next_attempt_at`. It answers 202 and emits `WagerTransactionPendingReference` **once**.
- **Worker** (`ResolvePendingReferences`), on every instance:
  - **Claim:** `UPDATE … SET next_attempt_at = now + lease WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)`. Instances don't collide, and a crashed worker's lease simply expires.
  - **Resolve:** each claimed row in its own transaction: lock the wallet, re-read the transaction, skip it unless it is still pending, then run the **same processor** as the submit path.
- **Backoff:** `2s · 2^(n-1)`, capped at 5 min, with jitter. The default budget is **10 attempts (~15–25 minutes)**.
  - **Why that budget:** providers resend a missing `BET` within seconds to minutes. Keeping a reversal open longer only delays their own reconciliation.
  - **When it runs out:** `REJECTED` + `REFERENCE_NOT_FOUND` (or `REFERENCE_NOT_PROCESSED` if the reference exists but never settled), plus `WagerTransactionRejected`. All values are configurable through environment variables.
- **Wake-up.** When a transaction reaches a terminal state, dependents waiting on its `(provider, external id)` get `next_attempt_at = now`. An early `ROLLBACK` is therefore applied within one worker tick instead of waiting out its backoff.
- **Permanent errors.** A non-transient error in the worker marks the transaction `FAILED` (`UNEXPECTED_ERROR`), which is terminal and auditable, instead of retrying forever. Like any terminal outcome, this wakes the transaction's dependents, which then resolve as `REFERENCE_NOT_PROCESSED` on the next tick.

## 13. Observability & reconciliation

**Logs.**
- **Format:** one JSON object per line.
- **Context propagated through `AsyncLocalStorage`:** `correlationId` (from `X-Correlation-Id`, the SQS envelope, or generated), `messageId`, `walletId`, `providerId`, `transactionId`.
- **No sensitive data:** callers pass identifiers only. The logger also redacts keys such as `amount`, `balance` and `payload`, and strips the SQL text (which embeds bound values) from driver errors. No payload or amount ever reaches the logs.

**Metrics** (`GET /metrics`, Prometheus):

| Metric | Covers |
|---|---|
| `wagering_transactions_total{kind,status,source}` | Transactions by status |
| `wagering_duplicates_total{kind}` | Replays, inbox redeliveries, idempotency conflicts |
| `wagering_retries_total{operation,reason}` | Retries (use case, SQS, outbox, worker) |
| `wagering_dlq_messages_total{reason}` | DLQ |
| `wagering_lock_conflicts_total` | Lock timeouts, deadlocks, serialization failures, version conflicts |
| `wagering_outbox_lag_seconds`, `wagering_outbox_pending` | Outbox lag |
| `wagering_processing_duration_seconds{source}` | Processing latency |
| `wagering_sqs_messages_total{outcome}`, `wagering_outbox_published_total`, `wagering_pending_reference_attempts_total`, `wagering_reconciliations_total{result}` | Diagnostics |

**Health.**
- `/health/live` checks only that the process responds, so a database outage cannot cause a restart storm.
- `/health/ready` runs `SELECT 1` and an SQS `GetQueueAttributes` with a 2 s timeout, and returns 503 if either fails.
- Both endpoints are `@Public()`.

**Reconciliation.**
- **Consistent snapshot.** It runs `REPEATABLE READ` and read-only, so the wallet and the ledger come from the same snapshot without blocking writers.
- **Comparison.** It compares the stored balance with `Σ credits − Σ debits`.
- **Mismatches** are logged at error level and counted (`result="mismatch"`), and the response says `consistent: false`. Nothing is ever corrected silently.
- **Mismatches should be impossible by construction**, because the deferred triggers would reject such a commit. This endpoint is the operational proof.

## 14. Authentication

**Not implemented, deliberately.** It carries no points, and a home-made scheme is explicitly unwanted. The extension point is explicit:

- **`ProviderIdentityPort`** with a `NoopProviderIdentity` adapter, and a global `AuthGuard`.
- **`@Public()`** marks health and metrics, so they stay open even once real authentication is plugged in.

**Design I would adopt:**
- **IdP.** Keycloak (or Zitadel) in Compose, with one confidential client per provider (client-credentials flow).
- **Token validation.** An `OidcProviderIdentity` adapter validates the JWT signature against the JWKS (cached, with key rotation) and checks `iss`, `aud` and `exp`. It maps a claim such as `provider_id` to the principal.
- **Ownership check.** The controller requires `principal.providerId === body.providerId`. Otherwise it returns `403`, before any domain code runs.
- **SQS stays an internal trusted channel**, but its `providerId` still goes through the same domain validations (ownership of the wallet, references scoped by provider).

## 15. Load test

`bun run test:load` (`test/load/load-test.ts`) is a closed-loop load generator.

- **Clients.** N virtual providers send requests back-to-back.
- **Mix.** 63% `BET`, 18% `WIN`, 9% `LOSS`, and 10% resends of earlier requests (idempotent replays).
- **Hot wallet.** 20% of new operations hit one wallet.
- **Downstream consumer.** A simulated consumer drains `wager-events.fifo` and records every `eventId` it receives.
- **Pass/fail.** The run's events are exactly the outbox rows of the wallets the run created, read from PostgreSQL (`LOAD_DATABASE_URL`). The run **fails** (exit 1) if:
  - any of those events is still unpublished after `DRAIN_TIMEOUT_S` (60 s);
  - any of them was never received downstream within `DELIVERY_TIMEOUT_S` (60 s), checked event id by event id;
  - any wallet of the run fails reconciliation;
  - any request fails with a network error or an HTTP 5xx.
- **Negative check.** Pointed at a single instance with the outbox relay disabled, a run fails as expected, reporting the run's events as unpublished and undelivered, with example event ids.

**Run 1: the Docker Compose stack** (what an evaluator runs). The environment was:

- `docker compose up` on Docker Desktop (WSL 2), Windows 11, a single desktop machine;
- 3 app replicas behind nginx, PostgreSQL 17.6 and LocalStack 4.14.0, all on the same host;
- 64 clients, 50 wallets, 30 s, `TARGETS=http://localhost:3000`.

| Metric | Value |
|---|---|
| Requests | 23,270 in 30.2 s → **~770 req/s** |
| Latency p50 / p95 / p99 / max | **20 ms / 391 ms / 1.36 s / 3.6 s** |
| Error rate | **0%** (all 200; 2,336 replays included) |
| Outbox | 39,624 events from this run; **0 unpublished**, drained **0.5 s** after the load stopped; publish lag p50 0.33 s, p99 1.65 s, max 4.4 s |
| Delivery | **39,624 / 39,624** event ids received downstream, **0 missing, 0 duplicates** |
| Wallets reconciled | **50 / 50 consistent** |
| Result | **PASS** |

Behind nginx, each `/metrics` scrape reaches one replica, so server-side counters from that run are a sample. Set `METRICS_TARGETS` to every instance for exact totals.

**Run 2: bare metal** (3 `bun run src/main.ts` processes, native PostgreSQL 18.4, MiniStack, no Docker networking): **~1,376 req/s**, p50 11 ms, p95 316 ms, p99 584 ms, 0 errors, 50/50 consistent.

**Analysis.**

- **The tail latency comes from the hot wallet.** About 20% of traffic queues on a single row lock. p50 stays low because other wallets proceed in parallel; p95 and p99 reflect that queue. This is the cost of strict per-wallet serialization, which is the correct behaviour for money. Docker Desktop's network path (WSL 2 + nginx) explains most of the gap between the two runs.
- **Lock conflicts and retries were 0** in the server counters (sampled from one replica through nginx). Pessimistic locks wait instead of failing, and with one lock per transaction and a fixed lock order there is nothing to deadlock on.
- **The outbox keeps up on LocalStack.** On MiniStack the lag reached ~21 s, because that emulator's `SendMessageBatch` slows down linearly with queue depth: I measured about 12k msgs/s when empty and about 17 msgs/s at 30k messages. On LocalStack the relay (batches of 100, parallel SQS chunks, bulk `UPDATE`) stayed within seconds. Earlier relay versions (batches of 10, row-by-row updates) lagged 66 s on MiniStack.
- **What this number is not.** It is not a capacity figure for production: everything shares one desktop. The point of the experiment is that **correctness holds under sustained concurrent load**: 0 errors, every wallet reconciled, no duplicate event deliveries.

## 16. Testing strategy

| Suite | What it proves |
|---|---|
| `test/unit` (107 tests, no I/O) | `Money` (scale, no rounding, invalid inputs, currency conflicts); `Wallet` and ledger invariants; the state machine; all `BET`/`WIN`/`LOSS`/`REFUND`/`ROLLBACK` rules; idempotency (replay, divergent payload, inbox) through the real use case on an in-memory unit of work |
| `test/integration` | Migrations up/down/up; every schema constraint and trigger attacked with raw SQL; atomicity (failure at the last write rolls everything back; crash before commit leaves no trace); inbox under 20 concurrent deliveries; 4 concurrent relays publish each event exactly once; a relay crash between publish and commit republishes the same `eventId`; SQS ack/redelivery/DLQ/retry-then-success/max-attempts/graceful shutdown; the full HTTP contract |
| `test/concurrency` | Same `BET` 50× in parallel → one debit; 100.00 vs 2×80.00 repeated 25× → exactly one `PROCESSED`, one `INSUFFICIENT_FUNDS`, balance 20.00, one debit, and retries never re-debit; a 200-operation hot wallet; 20 wallets in parallel; early `ROLLBACK`/`REFUND` racing their `BET`s and 3 workers; **3 OS processes** sharing hot wallets over HTTP and SQS; **SIGKILL after commit, before ack**; **SIGKILL between publish and commit** with two surviving publishers; **kill all instances mid-traffic, restart, and prove consistency** |

- **Real infrastructure.** Every integration and concurrency test uses real PostgreSQL and a real SQS emulator. Crash points are reached deterministically through `FAULT_INJECTION`, which is inert unless set.
- **Final invariant.** Every scenario ends with `assertLedgerInvariant`, which uses exact `Money` arithmetic, never JS numbers. It checks that:
  - versions are consecutive: `1, 2, 3, …` from the OPENING entry, or `2, 3, …` for a wallet opened empty, ending at the wallet's version;
  - each entry chains on the previous one, and the running sum equals every `balance_after`;
  - the balance equals the sum of the ledger, and nothing is negative.

  A schema test proves the helper itself catches a forged version gap.

## 17. Trade-offs, limitations & next steps

- **Per-wallet serialization caps hot-wallet throughput.** A single wallet handles roughly 1/(lock hold time) operations per second. If one player's throughput ever matters more than strict ordering, the next step would be a single conditional `UPDATE … SET balance = balance - $1 WHERE balance >= $1 RETURNING` with the ledger chain computed from `RETURNING`. It is shorter to hold, but it moves rules out of the aggregate.
- **The relay holds row locks during the SQS call.** That is a deliberate choice for no-double-publish among live relays. The alternative is a lease column (claim, then publish without locks), which allows more parallelism but needs fencing.
- **Event ordering is best-effort.** Consumers must use `walletVersion`/`occurredAt` and de-duplicate by `eventId`.
- **Retention.** `inbox_messages` and published `outbox_messages` grow forever. Production needs a TTL job: delete inbox rows older than the maximum redelivery window and published outbox rows older than N days, or use partitions by month. The ledger is never pruned.
- **Single currency in practice (BRL),** but the model and the tests are multi-currency.
- **Pending-reference budget.** The defaults come from reasoning, not provider SLAs, so they should be tuned per provider.
- **Not built (out of scope):** double-entry bookkeeping against a house account, OpenTelemetry tracing, dashboards, and authentication (§14).
- **Environment notes.**
  - LocalStack is pinned to 4.14.0 (tokenless); MiniStack is supported via `SQS_EMULATOR_IMAGE`.
  - Under `bun test`, `expect(promise).rejects` could stall on `pg` promises, so the schema tests use try/catch.
  - Spawned instances use `process.execPath`, so a crash test kills the real process and not a shim.
  - Fault injection exits with `process.exit(137)` rather than a self-`SIGKILL`: inside a container the app is PID 1, and the kernel ignores signals PID 1 has no handler for.
