import { beforeEach, describe, expect, test } from 'bun:test';
import { ExternalTransactionConflictError, IdempotencyConflictError, InboxPayloadConflictError, WalletNotFoundError } from '../../src/application/errors';
import { PendingReferencePolicy } from '../../src/application/wagering/pending-reference-policy';
import { ResolvePendingReferencesUseCase } from '../../src/application/wagering/resolve-pending-references.use-case';
import {
  type SubmitWagerTransactionCommand,
  SubmitWagerTransactionUseCase,
} from '../../src/application/wagering/submit-wager-transaction.use-case';
import { WagerTransactionProcessor } from '../../src/application/wagering/wager-transaction-processor';
import { CreateWalletUseCase } from '../../src/application/wallet/create-wallet.use-case';
import { ReconcileWalletUseCase } from '../../src/application/wallet/reconcile-wallet.use-case';
import { Money } from '../../src/domain/money/money';
import { FailureCode } from '../../src/domain/wagering/failure-code';
import type { SubmittableKind } from '../../src/domain/wagering/wager-transaction';
import {
  FixedClock,
  InMemoryUnitOfWork,
  noFaults,
  noopMetrics,
  SequentialIds,
  silentLogger,
} from '../support/in-memory';

let uow: InMemoryUnitOfWork;
let clock: FixedClock;
let submit: SubmitWagerTransactionUseCase;
let resolver: ResolvePendingReferencesUseCase;
let walletId: string;
const PLAYER = 'player-1';

const walletIds = new SequentialIds();

async function openWallet(amount: string, currency = 'BRL', playerId = PLAYER): Promise<string> {
  const ids = walletIds;
  const create = new CreateWalletUseCase(uow, { next: () => `w-${ids.next()}` }, clock, noopMetrics, silentLogger);
  const view = await create.execute({
    playerId,
    initialBalance: Money.from({ amount, currency }),
    correlationId: 'c',
  });
  return view.id;
}

function cmd(
  kind: `${SubmittableKind}`,
  externalId: string,
  amount: string,
  extra: Partial<SubmitWagerTransactionCommand> & { currency?: string } = {},
): SubmitWagerTransactionCommand {
  const { currency = 'BRL', ...rest } = extra;
  return {
    idempotencyKey: `prov:${externalId}`,
    providerId: 'prov',
    externalTransactionId: externalId,
    playerId: PLAYER,
    walletId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind: kind as SubmittableKind,
    money: Money.from({ amount, currency }),
    correlationId: 'corr',
    source: 'http',
    ...rest,
  };
}

async function balanceOf(id = walletId): Promise<string> {
  return uow.transactional(async (s) => (await s.wallets.findById(id))!.balance.amount);
}

async function ledgerCount(id = walletId): Promise<number> {
  return uow.store.ledger.filter((e) => e.walletId === id).length;
}

async function assertReconciled(id = walletId): Promise<void> {
  const report = await new ReconcileWalletUseCase(uow, noopMetrics, silentLogger).execute(id);
  expect(report.consistent).toBe(true);
}

beforeEach(async () => {
  uow = new InMemoryUnitOfWork();
  clock = new FixedClock();
  const ids = new SequentialIds();
  const processor = new WagerTransactionProcessor(ids, new PendingReferencePolicy(3, 1_000, 10_000, () => 0.5));
  submit = new SubmitWagerTransactionUseCase(uow, processor, ids, clock, noopMetrics, silentLogger, noFaults);
  resolver = new ResolvePendingReferencesUseCase(uow, processor, clock, noopMetrics, silentLogger);
  walletId = await openWallet('100.00');
});

describe('BET', () => {
  test('debits the wallet with one DEBIT entry', async () => {
    const r = await submit.execute(cmd('BET', 'b1', '25.00'));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });
    expect(await ledgerCount()).toBe(2); // OPENING + BET
    await assertReconciled();
  });

  test('is rejected (INSUFFICIENT_FUNDS) without touching balance or ledger', async () => {
    const r = await submit.execute(cmd('BET', 'b1', '100.01'));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: FailureCode.InsufficientFunds });
    expect(await balanceOf()).toBe('100.00');
    expect(await ledgerCount()).toBe(1);
  });

  test('may consume the whole balance', async () => {
    const r = await submit.execute(cmd('BET', 'b1', '100.00'));
    expect(r.balance?.amount).toBe('0.00');
  });
});

describe('WIN', () => {
  test('credits, with or without a BET reference', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00'));
    expect((await submit.execute(cmd('WIN', 'w1', '30.00', { referenceExternalTransactionId: 'b1' }))).balance?.amount).toBe('120.00');
    expect((await submit.execute(cmd('WIN', 'w2', '1.00'))).balance?.amount).toBe('121.00');
    await assertReconciled();
  });

  test('referencing something other than a BET is rejected', async () => {
    await submit.execute(cmd('WIN', 'w1', '1.00'));
    const r = await submit.execute(cmd('WIN', 'w2', '1.00', { referenceExternalTransactionId: 'w1' }));
    expect(r.failureCode).toBe(FailureCode.ReferenceKindNotAllowed);
  });

  test('referencing a REJECTED bet is rejected', async () => {
    await submit.execute(cmd('BET', 'b1', '500.00'));
    const r = await submit.execute(cmd('WIN', 'w1', '1.00', { referenceExternalTransactionId: 'b1' }));
    expect(r.failureCode).toBe(FailureCode.ReferenceNotProcessed);
  });
});

describe('LOSS', () => {
  test('is PROCESSED without moving the balance or writing ledger', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00'));
    const r = await submit.execute(cmd('LOSS', 'l1', '0.00', { referenceExternalTransactionId: 'b1' }));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '90.00' } });
    expect(await ledgerCount()).toBe(2);
    expect(uow.store.outbox.some((e) => e.eventType === 'WagerTransactionProcessed' && (e.payload as { data: { kind: string } }).data.kind === 'LOSS')).toBe(true);
  });
});

describe('REFUND', () => {
  test('credits back a processed BET exactly once', async () => {
    await submit.execute(cmd('BET', 'b1', '40.00'));
    const r = await submit.execute(cmd('REFUND', 'r1', '40.00', { referenceExternalTransactionId: 'b1' }));
    expect(r).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
    const again = await submit.execute(cmd('REFUND', 'r2', '40.00', { referenceExternalTransactionId: 'b1' }));
    expect(again.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    expect(await balanceOf()).toBe('100.00');
    await assertReconciled();
  });

  test('must reference a BET', async () => {
    await submit.execute(cmd('WIN', 'w1', '5.00'));
    const r = await submit.execute(cmd('REFUND', 'r1', '5.00', { referenceExternalTransactionId: 'w1' }));
    expect(r.failureCode).toBe(FailureCode.ReferenceKindNotAllowed);
  });

  test('amount must equal the reference (no partial reversal)', async () => {
    await submit.execute(cmd('BET', 'b1', '40.00'));
    const r = await submit.execute(cmd('REFUND', 'r1', '39.99', { referenceExternalTransactionId: 'b1' }));
    expect(r.failureCode).toBe(FailureCode.ReversalAmountMismatch);
  });

  test('reference must share player, wallet, currency and round', async () => {
    await submit.execute(cmd('BET', 'b1', '40.00'));
    const otherRound = await submit.execute(
      cmd('REFUND', 'r1', '40.00', { referenceExternalTransactionId: 'b1', roundId: 'round-2' }),
    );
    expect(otherRound.failureCode).toBe(FailureCode.ReferenceMismatch);

    const other = await openWallet('10.00', 'BRL', 'player-2');
    const otherWallet = await submit.execute(
      cmd('REFUND', 'r2', '40.00', { referenceExternalTransactionId: 'b1', walletId: other, playerId: 'player-2' }),
    );
    expect(otherWallet.failureCode).toBe(FailureCode.ReferenceMismatch);
  });

  test('a reference is looked up within the same provider only', async () => {
    await submit.execute(cmd('BET', 'b1', '40.00'));
    const r = await submit.execute(
      cmd('REFUND', 'r1', '40.00', { referenceExternalTransactionId: 'b1', providerId: 'other', idempotencyKey: 'other:r1' }),
    );
    expect(r.status as string).toBe('PENDING_REFERENCE');
  });
});

describe('ROLLBACK', () => {
  test('of a BET credits; of a WIN debits; of a REFUND debits', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00')); // 90
    expect((await submit.execute(cmd('ROLLBACK', 'rb1', '10.00', { referenceExternalTransactionId: 'b1' }))).balance?.amount).toBe('100.00');

    await submit.execute(cmd('BET', 'b2', '10.00')); // 90
    await submit.execute(cmd('WIN', 'w2', '50.00', { referenceExternalTransactionId: 'b2' })); // 140
    expect((await submit.execute(cmd('ROLLBACK', 'rb2', '50.00', { referenceExternalTransactionId: 'w2' }))).balance?.amount).toBe('90.00');

    await submit.execute(cmd('REFUND', 'rf2', '10.00', { referenceExternalTransactionId: 'b2' })); // 100
    expect((await submit.execute(cmd('ROLLBACK', 'rb3', '10.00', { referenceExternalTransactionId: 'rf2' }))).balance?.amount).toBe('90.00');
    await assertReconciled();
  });

  test('cannot reverse the same transaction twice (by ROLLBACK or after a REFUND)', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00'));
    await submit.execute(cmd('ROLLBACK', 'rb1', '10.00', { referenceExternalTransactionId: 'b1' }));
    const twice = await submit.execute(cmd('ROLLBACK', 'rb2', '10.00', { referenceExternalTransactionId: 'b1' }));
    expect(twice.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    const refundAfterRollback = await submit.execute(cmd('REFUND', 'rf1', '10.00', { referenceExternalTransactionId: 'b1' }));
    expect(refundAfterRollback.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);
    expect(await balanceOf()).toBe('100.00');
  });

  test('that would make the balance negative is rejected with a distinct, auditable code', async () => {
    await submit.execute(cmd('WIN', 'w1', '50.00')); // 150
    await submit.execute(cmd('BET', 'b1', '120.00')); // 30
    const r = await submit.execute(cmd('ROLLBACK', 'rb1', '50.00', { referenceExternalTransactionId: 'w1' }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: FailureCode.ReversalInsufficientFunds });
    expect(FailureCode.ReversalInsufficientFunds).not.toBe(FailureCode.InsufficientFunds);
    expect(await balanceOf()).toBe('30.00');
  });

  test('cannot reference a LOSS or another ROLLBACK', async () => {
    await submit.execute(cmd('LOSS', 'l1', '0.00'));
    expect((await submit.execute(cmd('ROLLBACK', 'rb1', '0.01', { referenceExternalTransactionId: 'l1' }))).failureCode).toBe(
      FailureCode.ReferenceKindNotAllowed,
    );
  });
});

describe('out-of-order references', () => {
  test('a ROLLBACK before its BET waits as PENDING_REFERENCE, then applies when the BET arrives', async () => {
    const early = await submit.execute(cmd('ROLLBACK', 'rb1', '10.00', { referenceExternalTransactionId: 'b1' }));
    expect(early.status as string).toBe('PENDING_REFERENCE');
    expect(uow.store.outbox.map((e) => e.eventType)).toContain('WagerTransactionPendingReference');

    await submit.execute(cmd('BET', 'b1', '10.00'));
    clock.advance(60_000);
    await resolver.runOnce({ batchSize: 10, leaseMs: 30_000 });

    const replay = await submit.execute(cmd('ROLLBACK', 'rb1', '10.00', { referenceExternalTransactionId: 'b1' }));
    expect(replay).toMatchObject({ status: 'PROCESSED', idempotentReplay: true, balance: { amount: '100.00' } });
    await assertReconciled();
  });

  test('is REJECTED with REFERENCE_NOT_FOUND once the retry budget is exhausted', async () => {
    await submit.execute(cmd('REFUND', 'rf1', '10.00', { referenceExternalTransactionId: 'never' }));
    for (let i = 0; i < 5; i++) {
      clock.advance(60_000);
      await resolver.runOnce({ batchSize: 10, leaseMs: 30_000 });
    }
    const tx = [...uow.store.transactions.values()].find((t) => t.externalTransactionId === 'rf1')!;
    expect(tx.status as string).toBe('REJECTED');
    expect(tx.failureCode).toBe(FailureCode.ReferenceNotFound);
    expect(tx.referenceAttempts).toBe(2); // 3 attempts in total: 1 at arrival + 2 retries
    expect(uow.store.outbox.filter((e) => e.eventType === 'WagerTransactionRejected')).toHaveLength(1);
    expect(uow.store.outbox.filter((e) => e.eventType === 'WagerTransactionPendingReference')).toHaveLength(1);
  });
});

describe('currency and ownership', () => {
  test('a transaction in another currency is REJECTED (CURRENCY_MISMATCH)', async () => {
    const r = await submit.execute(cmd('BET', 'b1', '1.00', { currency: 'USD' }));
    expect(r).toMatchObject({ status: 'REJECTED', failureCode: FailureCode.CurrencyMismatch, balance: { currency: 'BRL' } });
    expect(await balanceOf()).toBe('100.00');
  });

  test('a player that does not own the wallet is REJECTED', async () => {
    const r = await submit.execute(cmd('BET', 'b1', '1.00', { playerId: 'intruder' }));
    expect(r.failureCode).toBe(FailureCode.PlayerWalletMismatch);
  });

  test('unknown wallet is an error, not a persisted transaction', async () => {
    await expect(submit.execute(cmd('BET', 'b1', '1.00', { walletId: 'nope' }))).rejects.toBeInstanceOf(WalletNotFoundError);
    expect(uow.store.transactions.size).toBe(1); // only the OPENING
  });
});

describe('idempotency', () => {
  test('identical request → original result with idempotentReplay=true (balance observed at the time)', async () => {
    const first = await submit.execute(cmd('BET', 'b1', '10.00'));
    await submit.execute(cmd('BET', 'b2', '10.00'));
    const replay = await submit.execute(cmd('BET', 'b1', '10.00'));
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(replay.balance?.amount).toBe('90.00');
    expect(await balanceOf()).toBe('80.00');
  });

  test('replays of rejections return the same rejection', async () => {
    const first = await submit.execute(cmd('BET', 'b1', '1000.00'));
    expect(await submit.execute(cmd('BET', 'b1', '1000.00'))).toEqual({ ...first, idempotentReplay: true });
  });

  test('same key with a different payload is a conflict, not a replay', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00'));
    await expect(submit.execute(cmd('BET', 'b1', '10.01'))).rejects.toBeInstanceOf(IdempotencyConflictError);
    await expect(submit.execute(cmd('BET', 'b1', '10.00', { roundId: 'other' }))).rejects.toBeInstanceOf(
      IdempotencyConflictError,
    );
    expect(await balanceOf()).toBe('90.00');
  });

  test('normalized amounts hash equally ("10.5" ≡ "10.50")', async () => {
    await submit.execute(cmd('BET', 'b1', '10.50'));
    const replay = await submit.execute(cmd('BET', 'b1', '10.5'));
    expect(replay.idempotentReplay).toBe(true);
  });

  test('same provider transaction under a different key is a conflict', async () => {
    await submit.execute(cmd('BET', 'b1', '10.00'));
    await expect(submit.execute(cmd('BET', 'b1', '10.00', { idempotencyKey: 'another-key' }))).rejects.toBeInstanceOf(
      ExternalTransactionConflictError,
    );
  });

  test('inbox: a redelivered message is a duplicate; same id with another body is a conflict', async () => {
    const inbox = { consumerName: 'c', messageId: 'm1', payloadHash: 'h1' };
    const first = await submit.execute(cmd('BET', 'b1', '10.00', { source: 'sqs', inbox }));
    const again = await submit.execute(cmd('BET', 'b1', '10.00', { source: 'sqs', inbox }));
    expect(again).toEqual({ ...first, idempotentReplay: true });
    await expect(
      submit.execute(cmd('BET', 'b1', '10.00', { source: 'sqs', inbox: { ...inbox, payloadHash: 'h2' } })),
    ).rejects.toBeInstanceOf(InboxPayloadConflictError);
    expect(await balanceOf()).toBe('90.00');
  });
});

describe('events', () => {
  test('balance changes emit WalletBalanceChanged; rejections do not', async () => {
    uow.store.outbox = [];
    await submit.execute(cmd('BET', 'b1', '10.00'));
    await submit.execute(cmd('BET', 'b2', '1000.00'));
    expect(uow.store.outbox.map((e) => e.eventType)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
      'WagerTransactionRejected',
    ]);
    const changed = uow.store.outbox[1]!.payload as { data: Record<string, unknown>; version: number };
    expect(changed.version).toBe(1);
    expect(changed.data).toMatchObject({
      direction: 'DEBIT',
      money: { amount: '10.00', currency: 'BRL' },
      balanceBefore: { amount: '100.00', currency: 'BRL' },
      balanceAfter: { amount: '90.00', currency: 'BRL' },
      walletVersion: 2,
    });
  });
});
