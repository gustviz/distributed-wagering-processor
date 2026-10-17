import { ConcurrencyConflictError } from '../../application/errors';
import type {
  InboxRepository,
  LedgerRepository,
  LedgerSummary,
  OutboxRepository,
  OutboxStats,
  PendingReferenceClaim,
  TransactionScope,
  WagerTransactionRepository,
  WalletRepository,
} from '../../application/ports';
import { InboxMessage } from '../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { Money } from '../../domain/money/money';
import type { FailureCode } from '../../domain/wagering/failure-code';
import {
  WagerTransaction,
  type WagerTransactionKind,
  type WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { type LedgerDirection, WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';
import type { Sql } from './sql';

/*
 * Data mappers between rows and domain objects. Domain objects are rebuilt
 * with their `rehydrate` factories (no transition validation); `numeric`
 * columns arrive as exact decimal strings and become `Money` without ever
 * passing through a JS number.
 */

/**
 * MikroORM installs pg type parsers that return timestamps as strings in
 * PostgreSQL's text format ("2026-10-15 17:28:54.75+00"); normalize to ISO-8601
 * before parsing instead of relying on engine-specific Date parsing.
 */
type DbDate = Date | string;
const toDate = (value: DbDate): Date => {
  if (value instanceof Date) return value;
  const iso = value.replace(' ', 'T').replace(/([+-]\d{2})(\d{2})?$/, (_, h: string, m?: string) => `${h}:${m ?? '00'}`);
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) throw new Error(`Unparseable timestamp from database: ${value}`);
  return date;
};
const toOptionalDate = (value: DbDate | null): Date | undefined => (value === null ? undefined : toDate(value));

// ---------------------------------------------------------------------------
// Wallets
// ---------------------------------------------------------------------------

interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance: string;
  version: number;
  created_at: DbDate;
  updated_at: DbDate;
}

const toWallet = (r: WalletRow): Wallet =>
  Wallet.rehydrate({
    id: r.id,
    playerId: r.player_id,
    currency: r.currency,
    balance: Money.from({ amount: r.balance, currency: r.currency }),
    version: r.version,
    createdAt: toDate(r.created_at),
    updatedAt: toDate(r.updated_at),
  });

export class SqlWalletRepository implements WalletRepository {
  constructor(private readonly sql: Sql) {}

  async findById(id: string): Promise<Wallet | undefined> {
    const row = await this.sql.one<WalletRow>('SELECT * FROM wallets WHERE id = ?', [id]);
    return row && toWallet(row);
  }

  async lockById(id: string): Promise<Wallet | undefined> {
    const row = await this.sql.one<WalletRow>('SELECT * FROM wallets WHERE id = ? FOR UPDATE', [id]);
    return row && toWallet(row);
  }

  async insert(wallet: Wallet): Promise<void> {
    await this.sql.run(
      `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [wallet.id, wallet.playerId, wallet.currency, wallet.balance.amount, wallet.version, wallet.createdAt, wallet.updatedAt],
    );
  }

  async saveBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const affected = await this.sql.run(
      `UPDATE wallets SET balance = ?, version = ?, updated_at = ?
        WHERE id = ? AND version = ?`,
      [wallet.balance.amount, wallet.version, wallet.updatedAt, wallet.id, expectedVersion],
    );
    if (affected !== 1) throw new ConcurrencyConflictError('wallet', wallet.id);
  }
}

// ---------------------------------------------------------------------------
// Wager transactions
// ---------------------------------------------------------------------------

interface WagerTransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: string;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: string;
  failure_code: string | null;
  observed_balance: string | null;
  observed_currency: string | null;
  reference_attempts: number;
  next_attempt_at: DbDate | null;
  created_at: DbDate;
  updated_at: DbDate;
  processed_at: DbDate | null;
}

const toWagerTransaction = (r: WagerTransactionRow): WagerTransaction =>
  WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.provider_id,
    externalTransactionId: r.external_transaction_id,
    idempotencyKey: r.idempotency_key,
    payloadHash: r.payload_hash,
    walletId: r.wallet_id,
    playerId: r.player_id,
    roundId: r.round_id,
    gameId: r.game_id,
    kind: r.kind as WagerTransactionKind,
    money: Money.from({ amount: r.amount, currency: r.currency }),
    referenceExternalTransactionId: r.reference_external_transaction_id ?? undefined,
    referenceTransactionId: r.reference_transaction_id ?? undefined,
    status: r.status as WagerTransactionStatus,
    failureCode: (r.failure_code as FailureCode | null) ?? undefined,
    // The observed balance is the wallet's: its currency differs from the
    // transaction's on CURRENCY_MISMATCH rejections, hence its own column.
    observedBalance:
      r.observed_balance === null || r.observed_currency === null
        ? undefined
        : Money.from({ amount: r.observed_balance, currency: r.observed_currency }),
    referenceAttempts: r.reference_attempts,
    nextAttemptAt: toOptionalDate(r.next_attempt_at),
    createdAt: toDate(r.created_at),
    updatedAt: toDate(r.updated_at),
    processedAt: toOptionalDate(r.processed_at),
  });

export class SqlWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly sql: Sql) {}

  async findById(id: string): Promise<WagerTransaction | undefined> {
    return this.first('SELECT * FROM wager_transactions WHERE id = ?', [id]);
  }

  async findByExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined> {
    return this.first('SELECT * FROM wager_transactions WHERE provider_id = ? AND external_transaction_id = ?', [
      providerId,
      externalTransactionId,
    ]);
  }

  async findExisting(
    idempotencyKey: string,
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | undefined> {
    // Prefer the idempotency-key match: it decides replay vs. conflict.
    return this.first(
      `SELECT * FROM wager_transactions
        WHERE idempotency_key = ? OR (provider_id = ? AND external_transaction_id = ?)
        ORDER BY (idempotency_key = ?) DESC
        LIMIT 1`,
      [idempotencyKey, providerId, externalTransactionId, idempotencyKey],
    );
  }

  async lockById(id: string): Promise<WagerTransaction | undefined> {
    return this.first('SELECT * FROM wager_transactions WHERE id = ? FOR UPDATE', [id]);
  }

  async insert(tx: WagerTransaction): Promise<void> {
    await this.sql.run(
      `INSERT INTO wager_transactions (
         id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
         round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id,
         status, failure_code, observed_balance, observed_currency, reference_attempts, next_attempt_at,
         created_at, updated_at, processed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        tx.id,
        tx.providerId,
        tx.externalTransactionId,
        tx.idempotencyKey,
        tx.payloadHash,
        tx.walletId,
        tx.playerId,
        tx.roundId,
        tx.gameId,
        tx.kind,
        tx.money.amount,
        tx.money.currency,
        tx.referenceExternalTransactionId,
        tx.referenceTransactionId,
        tx.status,
        tx.failureCode,
        tx.observedBalance?.amount,
        tx.observedBalance?.currency,
        tx.referenceAttempts,
        tx.nextAttemptAt,
        tx.createdAt,
        tx.updatedAt,
        tx.processedAt,
      ],
    );
  }

  async update(tx: WagerTransaction): Promise<void> {
    await this.sql.run(
      `UPDATE wager_transactions
          SET status = ?, reference_transaction_id = ?, failure_code = ?, observed_balance = ?,
              observed_currency = ?, reference_attempts = ?, next_attempt_at = ?, updated_at = ?, processed_at = ?
        WHERE id = ?`,
      [
        tx.status,
        tx.referenceTransactionId,
        tx.failureCode,
        tx.observedBalance?.amount,
        tx.observedBalance?.currency,
        tx.referenceAttempts,
        tx.nextAttemptAt,
        tx.updatedAt,
        tx.processedAt,
        tx.id,
      ],
    );
  }

  async hasProcessedReversalOf(referenceTransactionId: string): Promise<boolean> {
    const row = await this.sql.one(
      `SELECT 1 AS found FROM wager_transactions
        WHERE reference_transaction_id = ? AND kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'
        LIMIT 1`,
      [referenceTransactionId],
    );
    return row !== undefined;
  }

  async wakeDependents(providerId: string, externalTransactionId: string, at: Date): Promise<number> {
    return this.sql.run(
      `UPDATE wager_transactions SET next_attempt_at = ?
        WHERE status = 'PENDING_REFERENCE' AND provider_id = ? AND reference_external_transaction_id = ?
          AND next_attempt_at > ?`,
      [at, providerId, externalTransactionId, at],
    );
  }

  async claimDuePendingReferences(now: Date, limit: number, leaseMs: number): Promise<PendingReferenceClaim[]> {
    const rows = await this.sql.rows<{ id: string; wallet_id: string }>(
      `UPDATE wager_transactions t
          SET next_attempt_at = ?
         FROM (SELECT id FROM wager_transactions
                WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= ?
                ORDER BY next_attempt_at
                LIMIT ?
                FOR UPDATE SKIP LOCKED) due
        WHERE t.id = due.id
        RETURNING t.id, t.wallet_id`,
      [new Date(now.getTime() + leaseMs), now, limit],
    );
    return rows.map((r) => ({ transactionId: r.id, walletId: r.wallet_id }));
  }

  private async first(query: string, params: (string | Date)[]): Promise<WagerTransaction | undefined> {
    const row = await this.sql.one<WagerTransactionRow>(query, params);
    return row && toWagerTransaction(row);
  }
}

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

interface LedgerRow {
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: string;
  amount: string;
  currency: string;
  balance_before: string;
  balance_after: string;
  wallet_version: number;
  created_at: DbDate;
}

const toLedgerEntry = (r: LedgerRow): WalletLedgerEntry =>
  WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.wallet_id,
    transactionId: r.transaction_id,
    direction: r.direction as LedgerDirection,
    money: Money.from({ amount: r.amount, currency: r.currency }),
    balanceBefore: Money.from({ amount: r.balance_before, currency: r.currency }),
    balanceAfter: Money.from({ amount: r.balance_after, currency: r.currency }),
    walletVersion: r.wallet_version,
    createdAt: toDate(r.created_at),
  });

export class SqlLedgerRepository implements LedgerRepository {
  constructor(private readonly sql: Sql) {}

  async append(entry: WalletLedgerEntry): Promise<void> {
    await this.sql.run(
      `INSERT INTO wallet_ledger_entries
         (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, wallet_version, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.amount,
        entry.money.currency,
        entry.balanceBefore.amount,
        entry.balanceAfter.amount,
        entry.walletVersion,
        entry.createdAt,
      ],
    );
  }

  async listByWallet(walletId: string, options: { beforeVersion?: number; limit: number }): Promise<WalletLedgerEntry[]> {
    const rows =
      options.beforeVersion === undefined
        ? await this.sql.rows<LedgerRow>(
            'SELECT * FROM wallet_ledger_entries WHERE wallet_id = ? ORDER BY wallet_version DESC LIMIT ?',
            [walletId, options.limit],
          )
        : await this.sql.rows<LedgerRow>(
            `SELECT * FROM wallet_ledger_entries
              WHERE wallet_id = ? AND wallet_version < ?
              ORDER BY wallet_version DESC LIMIT ?`,
            [walletId, options.beforeVersion, options.limit],
          );
    return rows.map(toLedgerEntry);
  }

  async summarize(walletId: string, currency: string): Promise<LedgerSummary> {
    const row = await this.sql.one<{ credits: string; debits: string; entries: number }>(
      `SELECT COALESCE(SUM(amount) FILTER (WHERE direction = 'CREDIT'), 0)::numeric(22,2)::text AS credits,
              COALESCE(SUM(amount) FILTER (WHERE direction = 'DEBIT'),  0)::numeric(22,2)::text AS debits,
              count(*)::int AS entries
         FROM wallet_ledger_entries
        WHERE wallet_id = ?`,
      [walletId],
    );
    return {
      credits: Money.from({ amount: row?.credits ?? '0.00', currency }),
      debits: Money.from({ amount: row?.debits ?? '0.00', currency }),
      entries: row?.entries ?? 0,
    };
  }
}

// ---------------------------------------------------------------------------
// Inbox
// ---------------------------------------------------------------------------

interface InboxRow {
  consumer_name: string;
  message_id: string;
  payload_hash: string;
  received_at: DbDate;
  processed_at: DbDate | null;
}

export class SqlInboxRepository implements InboxRepository {
  constructor(private readonly sql: Sql) {}

  async tryReceive(message: InboxMessage): Promise<InboxMessage | undefined> {
    const inserted = await this.sql.rows(
      `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
       VALUES (?, ?, ?, ?)
       ON CONFLICT (consumer_name, message_id) DO NOTHING
       RETURNING message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt],
    );
    if (inserted.length > 0) return undefined;
    const row = await this.sql.one<InboxRow>(
      'SELECT * FROM inbox_messages WHERE consumer_name = ? AND message_id = ?',
      [message.consumerName, message.messageId],
    );
    if (!row) throw new Error('Inbox conflict without a visible row');
    return InboxMessage.rehydrate({
      consumerName: row.consumer_name,
      messageId: row.message_id,
      payloadHash: row.payload_hash,
      receivedAt: toDate(row.received_at),
      processedAt: toOptionalDate(row.processed_at),
    });
  }

  async save(message: InboxMessage): Promise<void> {
    await this.sql.run('UPDATE inbox_messages SET processed_at = ? WHERE consumer_name = ? AND message_id = ?', [
      message.processedAt,
      message.consumerName,
      message.messageId,
    ]);
  }
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  occurred_at: DbDate;
  attempts: number;
  next_attempt_at: DbDate | null;
  published_at: DbDate | null;
  last_error: string | null;
}

export class SqlOutboxRepository implements OutboxRepository {
  constructor(private readonly sql: Sql) {}

  async add(messages: readonly OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    const placeholders = messages.map(() => '(?, ?, ?, ?::jsonb, ?, ?)').join(', ');
    const params = messages.flatMap((m) => [
      m.id,
      m.aggregateId,
      m.eventType,
      JSON.stringify(m.payload),
      m.occurredAt,
      m.attempts,
    ]);
    await this.sql.run(
      `INSERT INTO outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts) VALUES ${placeholders}`,
      params,
    );
  }

  async claimDue(now: Date, limit: number): Promise<OutboxMessage[]> {
    const rows = await this.sql.rows<OutboxRow>(
      `SELECT id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at, last_error
         FROM outbox_messages
        WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
        ORDER BY seq
        LIMIT ?
        FOR UPDATE SKIP LOCKED`,
      [now, limit],
    );
    return rows.map((r) =>
      OutboxMessage.rehydrate({
        id: r.id,
        aggregateId: r.aggregate_id,
        eventType: r.event_type,
        payload: r.payload,
        occurredAt: toDate(r.occurred_at),
        attempts: r.attempts,
        nextAttemptAt: toOptionalDate(r.next_attempt_at),
        publishedAt: toOptionalDate(r.published_at),
        lastError: r.last_error ?? undefined,
      }),
    );
  }

  async save(messages: readonly OutboxMessage[]): Promise<void> {
    // Happy path in one statement: all published rows of a batch share the same timestamp.
    const published = messages.filter((m) => !m.isPending());
    const publishedAt = published[0]?.publishedAt;
    if (publishedAt && published.every((m) => m.publishedAt?.getTime() === publishedAt.getTime())) {
      await this.sql.run(
        `UPDATE outbox_messages SET published_at = ?, next_attempt_at = NULL, last_error = NULL
          WHERE id IN (${published.map(() => '?').join(', ')})`,
        [publishedAt, ...published.map((m) => m.id)],
      );
    } else {
      for (const m of published) await this.update(m);
    }
    for (const m of messages.filter((m) => m.isPending())) await this.update(m);
  }

  private async update(m: OutboxMessage): Promise<void> {
    await this.sql.run(
      `UPDATE outbox_messages SET attempts = ?, next_attempt_at = ?, published_at = ?, last_error = ? WHERE id = ?`,
      [m.attempts, m.nextAttemptAt, m.publishedAt, m.lastError, m.id],
    );
  }

  async stats(now: Date): Promise<OutboxStats> {
    const row = await this.sql.one<{ pending: number; oldest: Date | null }>(
      'SELECT count(*)::int AS pending, min(occurred_at) AS oldest FROM outbox_messages WHERE published_at IS NULL',
    );
    const oldest = row?.oldest ? toDate(row.oldest) : undefined;
    return {
      pending: row?.pending ?? 0,
      oldestPendingAgeSeconds: oldest ? Math.max(0, (now.getTime() - oldest.getTime()) / 1000) : 0,
    };
  }
}

// ---------------------------------------------------------------------------

export class SqlTransactionScope implements TransactionScope {
  readonly wallets: WalletRepository;
  readonly transactions: WagerTransactionRepository;
  readonly ledger: LedgerRepository;
  readonly inbox: InboxRepository;
  readonly outbox: OutboxRepository;

  constructor(sql: Sql) {
    this.wallets = new SqlWalletRepository(sql);
    this.transactions = new SqlWagerTransactionRepository(sql);
    this.ledger = new SqlLedgerRepository(sql);
    this.inbox = new SqlInboxRepository(sql);
    this.outbox = new SqlOutboxRepository(sql);
  }
}
