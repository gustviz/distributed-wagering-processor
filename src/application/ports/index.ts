import type { InboxMessage } from '../../domain/messaging/inbox-message';
import type { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { Money } from '../../domain/money/money';
import type { WagerTransaction } from '../../domain/wagering/wager-transaction';
import type { Wallet } from '../../domain/wallet/wallet';
import type { WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';

// ---------------------------------------------------------------------------
// Injection tokens (ports are interfaces, so Nest needs explicit tokens)
// ---------------------------------------------------------------------------
export const CLOCK = Symbol('Clock');
export const ID_GENERATOR = Symbol('IdGenerator');
export const UNIT_OF_WORK = Symbol('UnitOfWork');
export const EVENT_PUBLISHER = Symbol('EventPublisher');
export const METRICS = Symbol('Metrics');
export const APP_LOGGER = Symbol('AppLogger');
export const FAULT_INJECTOR = Symbol('FaultInjector');
export const HEALTH_PROBES = Symbol('HealthProbes');

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  /** Time-ordered unique id (UUIDv7). */
  next(): string;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

export interface WalletRepository {
  findById(id: string): Promise<Wallet | undefined>;
  /** `SELECT ... FOR UPDATE`: serializes every writer of this wallet (and only this wallet). */
  lockById(id: string): Promise<Wallet | undefined>;
  insert(wallet: Wallet): Promise<void>;
  /**
   * Persists a balance change guarded by `WHERE version = expectedVersion`.
   * Throws ConcurrencyConflictError if the guard does not match.
   */
  saveBalance(wallet: Wallet, expectedVersion: number): Promise<void>;
}

export interface PendingReferenceClaim {
  transactionId: string;
  walletId: string;
}

export interface WagerTransactionRepository {
  findById(id: string): Promise<WagerTransaction | undefined>;
  findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  /** Existing transaction holding this idempotency key or this (provider, external id). */
  findExisting(
    idempotencyKey: string,
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | undefined>;
  lockById(id: string): Promise<WagerTransaction | undefined>;
  insert(tx: WagerTransaction): Promise<void>;
  update(tx: WagerTransaction): Promise<void>;
  hasProcessedReversalOf(referenceTransactionId: string): Promise<boolean>;
  /** Brings forward the next attempt of transactions waiting for this (provider, external id). */
  wakeDependents(providerId: string, externalTransactionId: string, at: Date): Promise<number>;
  /**
   * Leases due PENDING_REFERENCE transactions (FOR UPDATE SKIP LOCKED + push
   * next_attempt_at forward) so concurrent workers do not pick the same rows.
   */
  claimDuePendingReferences(now: Date, limit: number, leaseMs: number): Promise<PendingReferenceClaim[]>;
}

export interface LedgerSummary {
  credits: Money;
  debits: Money;
  entries: number;
}

export interface LedgerRepository {
  append(entry: WalletLedgerEntry): Promise<void>;
  /** Newest first; `beforeVersion` is the exclusive upper bound (keyset pagination). */
  listByWallet(walletId: string, options: { beforeVersion?: number; limit: number }): Promise<WalletLedgerEntry[]>;
  summarize(walletId: string, currency: string): Promise<LedgerSummary>;
}

export interface InboxRepository {
  /**
   * Inserts the inbox row (`ON CONFLICT DO NOTHING`). Returns `undefined` when this
   * call acquired the message, or the already-committed row when it is a redelivery.
   * A concurrent duplicate blocks on the unique key until the first commits/aborts.
   */
  tryReceive(message: InboxMessage): Promise<InboxMessage | undefined>;
  save(message: InboxMessage): Promise<void>;
}

export interface OutboxStats {
  pending: number;
  oldestPendingAgeSeconds: number;
}

export interface OutboxRepository {
  add(messages: readonly OutboxMessage[]): Promise<void>;
  /** Due, unpublished rows locked with `FOR UPDATE SKIP LOCKED`. Must run inside a transaction. */
  claimDue(now: Date, limit: number): Promise<OutboxMessage[]>;
  save(messages: readonly OutboxMessage[]): Promise<void>;
  stats(now: Date): Promise<OutboxStats>;
}

export interface TransactionScope {
  readonly wallets: WalletRepository;
  readonly transactions: WagerTransactionRepository;
  readonly ledger: LedgerRepository;
  readonly inbox: InboxRepository;
  readonly outbox: OutboxRepository;
}

export interface TransactionOptions {
  isolation?: 'read committed' | 'repeatable read';
  readOnly?: boolean;
}

export interface UnitOfWork {
  /** Runs `work` in one SQL transaction: everything it writes commits or rolls back together. */
  transactional<T>(work: (scope: TransactionScope) => Promise<T>, options?: TransactionOptions): Promise<T>;
}

// ---------------------------------------------------------------------------
// Messaging / observability / testing seams
// ---------------------------------------------------------------------------

export interface PublishResult {
  id: string;
  ok: boolean;
  error?: string;
}

export interface EventPublisher {
  publish(messages: readonly OutboxMessage[]): Promise<PublishResult[]>;
}

export type TransactionSource = 'http' | 'sqs' | 'worker' | 'internal';

export interface Metrics {
  transactionFinished(kind: string, status: string, source: TransactionSource): void;
  processingDuration(source: TransactionSource, seconds: number): void;
  duplicateDetected(kind: 'idempotent_replay' | 'inbox_redelivery' | 'idempotency_conflict'): void;
  retry(operation: string, reason: string): void;
  lockConflict(): void;
  dlqMessage(reason: string): void;
  sqsMessage(outcome: string): void;
  outboxPublished(count: number): void;
  outboxPublishFailed(count: number): void;
  outboxState(stats: OutboxStats): void;
  pendingReferenceResolution(outcome: string): void;
  reconciliation(consistent: boolean): void;
}

export type LogFields = Record<string, string | number | boolean | undefined | null>;

/** Structured logger. Callers pass identifiers only — never full payloads or amounts. */
export interface AppLogger {
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  error(message: string, fields?: LogFields, err?: unknown): void;
}

/**
 * Named crash points used by tests to kill a process at precise moments
 * (e.g. after commit, before SQS ack). A no-op unless FAULT_INJECTION is set.
 */
export interface FaultInjector {
  checkpoint(name: FaultPoint): Promise<void>;
}

export type FaultPoint =
  | 'sqs.after-commit-before-ack'
  | 'outbox.after-publish-before-commit'
  | 'submit.before-commit';

export interface HealthProbes {
  database(): Promise<void>;
  queue(): Promise<void>;
}
