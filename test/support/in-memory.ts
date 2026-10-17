import { ConcurrencyConflictError, UniqueViolationError } from '../../src/application/errors';
import type {
  AppLogger,
  Clock,
  FaultInjector,
  IdGenerator,
  InboxRepository,
  LedgerRepository,
  Metrics,
  OutboxRepository,
  TransactionScope,
  UnitOfWork,
  WagerTransactionRepository,
  WalletRepository,
} from '../../src/application/ports';
import { InboxMessage, type InboxMessageState } from '../../src/domain/messaging/inbox-message';
import { OutboxMessage, type OutboxMessageState } from '../../src/domain/messaging/outbox-message';
import { Money } from '../../src/domain/money/money';
import { WagerTransaction, type WagerTransactionState } from '../../src/domain/wagering/wager-transaction';
import { Wallet, type WalletState } from '../../src/domain/wallet/wallet';
import { type LedgerEntryState, WalletLedgerEntry } from '../../src/domain/wallet/wallet-ledger-entry';

/**
 * In-memory adapter for fast unit tests of the application rules.
 * Stores plain states (never live aggregates) and rolls back on error, so a
 * failed "transaction" leaves no trace — like the real adapter. It is NOT a
 * substitute for the PostgreSQL tests: no concurrency, no SQL constraints.
 */
export class InMemoryStore {
  wallets = new Map<string, WalletState>();
  transactions = new Map<string, WagerTransactionState>();
  ledger: LedgerEntryState[] = [];
  inbox = new Map<string, InboxMessageState>();
  outbox: OutboxMessageState[] = [];

  snapshot() {
    return structuredClone({
      wallets: [...this.wallets],
      transactions: [...this.transactions],
      ledger: this.ledger,
      inbox: [...this.inbox],
      outbox: this.outbox,
    });
  }

  restore(s: ReturnType<InMemoryStore['snapshot']>) {
    this.wallets = new Map(s.wallets);
    this.transactions = new Map(s.transactions);
    this.ledger = s.ledger;
    this.inbox = new Map(s.inbox);
    this.outbox = s.outbox;
  }
}

// structuredClone drops class prototypes; rebuild Money values on read.
const money = (m: Money | { minor?: unknown; currency: string } | undefined): Money | undefined =>
  m === undefined ? undefined : m instanceof Money ? m : Money.fromSigned(moneyProps(m));
function moneyProps(m: unknown): { amount: string; currency: string } {
  const raw = m as { minor: bigint; currency: string };
  const negative = raw.minor < 0n;
  const abs = negative ? -raw.minor : raw.minor;
  return { amount: `${negative ? '-' : ''}${abs / 100n}.${(abs % 100n).toString().padStart(2, '0')}`, currency: raw.currency };
}
const walletOf = (s: WalletState) => Wallet.rehydrate({ ...s, balance: money(s.balance)! });
const txOf = (s: WagerTransactionState) =>
  WagerTransaction.rehydrate({ ...s, money: money(s.money)!, observedBalance: money(s.observedBalance) });
const entryOf = (s: LedgerEntryState) =>
  WalletLedgerEntry.rehydrate({
    ...s,
    money: money(s.money)!,
    balanceBefore: money(s.balanceBefore)!,
    balanceAfter: money(s.balanceAfter)!,
  });

class Scope implements TransactionScope {
  constructor(private readonly store: InMemoryStore) {}

  readonly wallets: WalletRepository = {
    findById: async (id) => {
      const s = this.store.wallets.get(id);
      return s && walletOf(s);
    },
    lockById: async (id) => this.wallets.findById(id),
    insert: async (wallet) => {
      for (const w of this.store.wallets.values()) {
        if (w.playerId === wallet.playerId && w.currency === wallet.currency) {
          throw new UniqueViolationError('uq_wallets_player_currency');
        }
      }
      this.store.wallets.set(wallet.id, wallet.toState());
    },
    saveBalance: async (wallet, expectedVersion) => {
      const current = this.store.wallets.get(wallet.id);
      if (!current || current.version !== expectedVersion) throw new ConcurrencyConflictError('wallet', wallet.id);
      this.store.wallets.set(wallet.id, wallet.toState());
    },
  };

  readonly transactions: WagerTransactionRepository = {
    findById: async (id) => {
      const s = this.store.transactions.get(id);
      return s && txOf(s);
    },
    findByExternalId: async (providerId, externalId) => {
      for (const s of this.store.transactions.values()) {
        if (s.providerId === providerId && s.externalTransactionId === externalId) return txOf(s);
      }
      return undefined;
    },
    findExisting: async (key, providerId, externalId) => {
      for (const s of this.store.transactions.values()) if (s.idempotencyKey === key) return txOf(s);
      return this.transactions.findByExternalId(providerId, externalId);
    },
    lockById: async (id) => this.transactions.findById(id),
    insert: async (tx) => {
      for (const s of this.store.transactions.values()) {
        if (s.idempotencyKey === tx.idempotencyKey) throw new UniqueViolationError('uq_wager_tx_idempotency_key');
        if (s.providerId === tx.providerId && s.externalTransactionId === tx.externalTransactionId) {
          throw new UniqueViolationError('uq_wager_tx_provider_external');
        }
      }
      this.store.transactions.set(tx.id, tx.toState());
    },
    update: async (tx) => {
      this.store.transactions.set(tx.id, tx.toState());
    },
    hasProcessedReversalOf: async (refId) =>
      [...this.store.transactions.values()].some(
        (s) => s.referenceTransactionId === refId && (s.kind === 'REFUND' || s.kind === 'ROLLBACK') && s.status === 'PROCESSED',
      ),
    wakeDependents: async () => 0,
    claimDuePendingReferences: async (now, limit) =>
      [...this.store.transactions.values()]
        .filter((s) => s.status === 'PENDING_REFERENCE' && s.nextAttemptAt && s.nextAttemptAt <= now)
        .slice(0, limit)
        .map((s) => ({ transactionId: s.id, walletId: s.walletId })),
  };

  readonly ledger: LedgerRepository = {
    append: async (entry) => {
      this.store.ledger.push({ ...entry });
    },
    listByWallet: async (walletId, { beforeVersion, limit }) =>
      this.store.ledger
        .filter((e) => e.walletId === walletId && (beforeVersion === undefined || e.walletVersion < beforeVersion))
        .sort((a, b) => b.walletVersion - a.walletVersion)
        .slice(0, limit)
        .map(entryOf),
    summarize: async (walletId, currency) => {
      const entries = this.store.ledger.filter((e) => e.walletId === walletId).map(entryOf);
      let credits = Money.zero(currency);
      let debits = Money.zero(currency);
      for (const e of entries) {
        if (e.direction === 'CREDIT') credits = credits.add(e.money);
        else debits = debits.add(e.money);
      }
      return { credits, debits, entries: entries.length };
    },
  };

  readonly inbox: InboxRepository = {
    tryReceive: async (message) => {
      const key = `${message.consumerName}/${message.messageId}`;
      const existing = this.store.inbox.get(key);
      if (existing) return InboxMessage.rehydrate(existing);
      this.store.inbox.set(key, { ...message, processedAt: message.processedAt });
      return undefined;
    },
    save: async (message) => {
      this.store.inbox.set(`${message.consumerName}/${message.messageId}`, {
        ...message,
        processedAt: message.processedAt,
      });
    },
  };

  readonly outbox: OutboxRepository = {
    add: async (messages) => {
      this.store.outbox.push(...messages.map((m) => m.toState()));
    },
    claimDue: async (now, limit) =>
      this.store.outbox
        .filter((m) => !m.publishedAt && (!m.nextAttemptAt || m.nextAttemptAt <= now))
        .slice(0, limit)
        .map((m) => OutboxMessage.rehydrate(m)),
    save: async (messages) => {
      for (const m of messages) {
        const i = this.store.outbox.findIndex((o) => o.id === m.id);
        this.store.outbox[i] = m.toState();
      }
    },
    stats: async () => ({ pending: this.store.outbox.filter((m) => !m.publishedAt).length, oldestPendingAgeSeconds: 0 }),
  };
}

export class InMemoryUnitOfWork implements UnitOfWork {
  constructor(readonly store = new InMemoryStore()) {}

  async transactional<T>(work: (scope: TransactionScope) => Promise<T>): Promise<T> {
    const before = this.store.snapshot();
    try {
      return await work(new Scope(this.store));
    } catch (error) {
      this.store.restore(before);
      throw error;
    }
  }
}

export class FixedClock implements Clock {
  constructor(public current = new Date('2026-07-29T15:00:00.000Z')) {}
  now(): Date {
    return new Date(this.current);
  }
  advance(ms: number): void {
    this.current = new Date(this.current.getTime() + ms);
  }
}

export class SequentialIds implements IdGenerator {
  private n = 0;
  next(): string {
    this.n++;
    return `00000000-0000-7000-8000-${this.n.toString(16).padStart(12, '0')}`;
  }
}

export const silentLogger: AppLogger = { debug() {}, info() {}, warn() {}, error() {} };

export const noopMetrics: Metrics = {
  transactionFinished() {},
  processingDuration() {},
  duplicateDetected() {},
  retry() {},
  lockConflict() {},
  dlqMessage() {},
  sqsMessage() {},
  outboxPublished() {},
  outboxPublishFailed() {},
  outboxState() {},
  pendingReferenceResolution() {},
  reconciliation() {},
};

export const noFaults: FaultInjector = { checkpoint: async () => undefined };
