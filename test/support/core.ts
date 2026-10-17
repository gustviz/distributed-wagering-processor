import { randomUUID } from 'node:crypto';
import { MikroORM } from '@mikro-orm/postgresql';
import { RelayOutboxUseCase } from '../../src/application/messaging/relay-outbox.use-case';
import type { EventPublisher, FaultInjector, UnitOfWork } from '../../src/application/ports';
import { PendingReferencePolicy } from '../../src/application/wagering/pending-reference-policy';
import { ResolvePendingReferencesUseCase } from '../../src/application/wagering/resolve-pending-references.use-case';
import {
  type SubmitWagerTransactionCommand,
  SubmitWagerTransactionUseCase,
} from '../../src/application/wagering/submit-wager-transaction.use-case';
import { WagerTransactionProcessor } from '../../src/application/wagering/wager-transaction-processor';
import { CreateWalletUseCase } from '../../src/application/wallet/create-wallet.use-case';
import { ReconcileWalletUseCase } from '../../src/application/wallet/reconcile-wallet.use-case';
import { WalletQueries } from '../../src/application/wallet/wallet-queries';
import { Money } from '../../src/domain/money/money';
import type { SubmittableKind } from '../../src/domain/wagering/wager-transaction';
import { PrometheusMetrics } from '../../src/infrastructure/observability/prometheus-metrics';
import { MikroOrmUnitOfWork } from '../../src/infrastructure/persistence/mikro-orm-unit-of-work';
import { ormConfig } from '../../src/infrastructure/persistence/mikro-orm.config';
import { SystemClock, UuidV7Generator } from '../../src/infrastructure/support';
import { prepareDatabase } from './database';
import { TEST_DATABASE_URL } from './env';
import { noFaults, silentLogger } from './in-memory';

export interface CoreOptions {
  poolMax?: number;
  faults?: FaultInjector;
  publisher?: EventPublisher;
  policy?: PendingReferencePolicy;
  wrapUnitOfWork?: (uow: UnitOfWork) => UnitOfWork;
}

/** The application core wired to the REAL PostgreSQL adapter (no HTTP, no Nest). */
export async function createCore(options: CoreOptions = {}) {
  await prepareDatabase();
  const orm = await MikroORM.init(ormConfig({ url: TEST_DATABASE_URL, poolMax: options.poolMax ?? 30 }));
  const baseUow = new MikroOrmUnitOfWork(orm, { lockTimeoutMs: 5_000, statementTimeoutMs: 15_000 });
  const uow = options.wrapUnitOfWork ? options.wrapUnitOfWork(baseUow) : baseUow;
  const ids = new UuidV7Generator();
  const clock = new SystemClock();
  const metrics = new PrometheusMetrics();
  const faults = options.faults ?? noFaults;
  const processor = new WagerTransactionProcessor(ids, options.policy ?? new PendingReferencePolicy(20, 50, 200));
  const recordingPublisher: EventPublisher = {
    publish: async (messages) => messages.map((m) => ({ id: m.id, ok: true })),
  };
  return {
    orm,
    uow,
    metrics,
    submit: new SubmitWagerTransactionUseCase(uow, processor, ids, clock, metrics, silentLogger, faults),
    createWallet: new CreateWalletUseCase(uow, ids, clock, metrics, silentLogger),
    queries: new WalletQueries(uow),
    reconcile: new ReconcileWalletUseCase(uow, metrics, silentLogger),
    resolver: new ResolvePendingReferencesUseCase(uow, processor, clock, metrics, silentLogger),
    relay: new RelayOutboxUseCase(uow, options.publisher ?? recordingPublisher, clock, metrics, silentLogger, faults, {
      baseDelayMs: 50,
      maxDelayMs: 200,
    }),
    close: () => orm.close(true),
  };
}

export type Core = Awaited<ReturnType<typeof createCore>>;

export function uniq(prefix: string): string {
  return `${prefix}-${randomUUID().slice(0, 12)}`;
}

export async function newWallet(core: Core, amount = '100.00', currency = 'BRL') {
  const playerId = uniq('player');
  const wallet = await core.createWallet.execute({
    playerId,
    initialBalance: Money.from({ amount, currency }),
    correlationId: 'test',
  });
  return { walletId: wallet.id, playerId };
}

/** Wire-format payload (as HTTP/SQS would carry it). */
export function wagerPayload(
  w: { walletId: string; playerId: string },
  kind: `${SubmittableKind}`,
  externalTransactionId: string,
  amount: string,
  extra: { providerId?: string; roundId?: string; reference?: string; currency?: string } = {},
) {
  return {
    providerId: extra.providerId ?? 'provider-a',
    externalTransactionId,
    playerId: w.playerId,
    walletId: w.walletId,
    roundId: extra.roundId ?? 'round-1',
    gameId: 'fortune-chimp',
    kind: kind as SubmittableKind,
    money: { amount, currency: extra.currency ?? 'BRL' },
    ...(extra.reference ? { referenceExternalTransactionId: extra.reference } : {}),
  };
}

export function command(
  w: { walletId: string; playerId: string },
  kind: `${SubmittableKind}`,
  externalTransactionId: string,
  amount: string,
  extra: Parameters<typeof wagerPayload>[4] & Partial<Pick<SubmitWagerTransactionCommand, 'inbox' | 'source'>> = {},
): SubmitWagerTransactionCommand {
  const p = wagerPayload(w, kind, externalTransactionId, amount, extra);
  return {
    idempotencyKey: `${p.providerId}:${externalTransactionId}`,
    ...p,
    money: Money.from(p.money),
    correlationId: 'test',
    source: extra.source ?? 'http',
    inbox: extra.inbox,
  };
}

export async function waitFor<T>(
  probe: () => Promise<T | undefined | false>,
  { timeoutMs = 20_000, intervalMs = 100, message = 'condition' } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await probe();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await Bun.sleep(intervalMs);
  }
  throw new Error(`Timed out waiting for ${message}${last ? `: ${String(last)}` : ''}`);
}
