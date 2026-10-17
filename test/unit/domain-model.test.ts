import { describe, expect, test } from 'bun:test';
import { canonicalJson, wagerPayloadHash } from '../../src/application/idempotency/payload-hash';
import { PendingReferencePolicy } from '../../src/application/wagering/pending-reference-policy';
import { WalletBalanceChanged } from '../../src/domain/events/wagering-events';
import { InboxMessage } from '../../src/domain/messaging/inbox-message';
import { OutboxMessage } from '../../src/domain/messaging/outbox-message';
import { Money } from '../../src/domain/money/money';
import { InvalidTransactionError, InvalidTransactionStateError } from '../../src/domain/shared/domain-error';
import { FailureCode } from '../../src/domain/wagering/failure-code';
import {
  type CreateWagerTransactionProps,
  type SubmittableKind,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction';
import { Wallet } from '../../src/domain/wallet/wallet';
import { LedgerDirection } from '../../src/domain/wallet/wallet-ledger-entry';

const at = new Date('2026-01-01T00:00:00Z');
const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

function props(kind: SubmittableKind, over: Partial<CreateWagerTransactionProps> = {}): CreateWagerTransactionProps {
  return {
    id: `id-${kind}-${Math.random()}`,
    providerId: 'p',
    externalTransactionId: `ext-${kind}`,
    idempotencyKey: `p:ext-${kind}`,
    payloadHash: 'a'.repeat(64),
    walletId: 'w',
    playerId: 'pl',
    roundId: 'r',
    gameId: 'g',
    kind,
    money: brl('10.00'),
    createdAt: at,
    ...over,
  };
}

describe('WagerTransaction — creation rules', () => {
  test('is born PENDING', () => {
    expect(WagerTransaction.create(props('BET' as SubmittableKind)).status).toBe(WagerTransactionStatus.Pending);
  });

  test('REFUND and ROLLBACK require a reference', () => {
    expect(() => WagerTransaction.create(props(WagerTransactionKind.Refund))).toThrow(InvalidTransactionError);
    expect(() => WagerTransaction.create(props(WagerTransactionKind.Rollback))).toThrow(InvalidTransactionError);
    expect(
      WagerTransaction.create(props(WagerTransactionKind.Refund, { referenceExternalTransactionId: 'b' })).requiresReference(),
    ).toBe(true);
  });

  test('BET cannot carry a reference; a transaction cannot reference itself', () => {
    expect(() =>
      WagerTransaction.create(props(WagerTransactionKind.Bet, { referenceExternalTransactionId: 'x' })),
    ).toThrow(InvalidTransactionError);
    expect(() =>
      WagerTransaction.create(
        props(WagerTransactionKind.Win, { referenceExternalTransactionId: 'ext-WIN', externalTransactionId: 'ext-WIN' }),
      ),
    ).toThrow(InvalidTransactionError);
  });

  test('OPENING cannot be created through the public factory', () => {
    expect(() => WagerTransaction.create(props('OPENING' as SubmittableKind))).toThrow(InvalidTransactionError);
  });

  test('amount must be > 0 (LOSS may be zero)', () => {
    expect(() => WagerTransaction.create(props(WagerTransactionKind.Bet, { money: brl('0.00') }))).toThrow(
      InvalidTransactionError,
    );
    expect(WagerTransaction.create(props(WagerTransactionKind.Loss, { money: brl('0.00') })).affectsBalance()).toBe(false);
  });
});

describe('WagerTransaction — state machine', () => {
  const pending = () => WagerTransaction.create(props(WagerTransactionKind.Bet));

  test.each([
    ['PROCESSED', (t: WagerTransaction) => t.markProcessed(undefined, at, brl('1.00'))],
    ['REJECTED', (t: WagerTransaction) => t.reject(FailureCode.InsufficientFunds, at, brl('1.00'))],
    ['FAILED', (t: WagerTransaction) => t.fail(FailureCode.UnexpectedError, at)],
  ])('%s is terminal: any further transition throws', (_, toTerminal) => {
    const tx = pending();
    toTerminal(tx);
    expect(tx.isTerminal()).toBe(true);
    expect(() => tx.markProcessed(undefined, at, brl('1.00'))).toThrow(InvalidTransactionStateError);
    expect(() => tx.reject(FailureCode.InsufficientFunds, at, undefined)).toThrow(InvalidTransactionStateError);
    expect(() => tx.fail(FailureCode.UnexpectedError, at)).toThrow(InvalidTransactionStateError);
    expect(() => tx.markPendingReference(at, at, brl('1.00'))).toThrow(InvalidTransactionStateError);
  });

  test('PENDING_REFERENCE can be retried and then decided', () => {
    const tx = WagerTransaction.create(props(WagerTransactionKind.Refund, { referenceExternalTransactionId: 'b' }));
    tx.markPendingReference(at, at, brl('1.00'));
    tx.markPendingReference(at, at, brl('1.00'));
    expect(tx.referenceAttempts).toBe(2);
    tx.markProcessed('ref-id', at, brl('11.00'));
    expect(tx.status).toBe(WagerTransactionStatus.Processed);
    expect(tx.referenceTransactionId).toBe('ref-id');
    expect(tx.nextAttemptAt).toBeUndefined();
  });

  test('matchesPayload compares hashes', () => {
    expect(pending().matchesPayload('a'.repeat(64))).toBe(true);
    expect(pending().matchesPayload('b'.repeat(64))).toBe(false);
  });
});

describe('WagerTransaction — ledger direction and reference assessment', () => {
  const processed = (kind: SubmittableKind, over: Partial<CreateWagerTransactionProps> = {}) => {
    const tx = WagerTransaction.create(props(kind, over));
    tx.markProcessed(undefined, at, brl('0.00'));
    return tx;
  };

  test('directions', () => {
    expect(processed(WagerTransactionKind.Bet).ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(processed(WagerTransactionKind.Win).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    const rollback = WagerTransaction.create(props(WagerTransactionKind.Rollback, { referenceExternalTransactionId: 'x' }));
    expect(rollback.ledgerDirectionFor(processed(WagerTransactionKind.Bet))).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(processed(WagerTransactionKind.Win))).toBe(LedgerDirection.Debit);
    expect(() => rollback.ledgerDirectionFor()).toThrow(InvalidTransactionError);
    expect(() => processed(WagerTransactionKind.Loss).ledgerDirectionFor()).toThrow(InvalidTransactionError);
  });

  test('assessReference: ownership, kind, state and amount', () => {
    const refund = WagerTransaction.create(props(WagerTransactionKind.Refund, { referenceExternalTransactionId: 'b' }));
    expect(refund.assessReference(processed(WagerTransactionKind.Bet))).toEqual({ type: 'accept' });
    expect(refund.assessReference(processed(WagerTransactionKind.Bet, { playerId: 'x' }))).toEqual({
      type: 'reject',
      code: FailureCode.ReferenceMismatch,
    });
    expect(refund.assessReference(processed(WagerTransactionKind.Win))).toEqual({
      type: 'reject',
      code: FailureCode.ReferenceKindNotAllowed,
    });
    expect(refund.assessReference(WagerTransaction.create(props(WagerTransactionKind.Bet)))).toEqual({ type: 'wait' });
    expect(refund.assessReference(processed(WagerTransactionKind.Bet, { money: brl('9.99') }))).toEqual({
      type: 'reject',
      code: FailureCode.ReversalAmountMismatch,
    });
  });
});

describe('payload hash', () => {
  const fields = {
    providerId: 'p',
    externalTransactionId: 'e',
    playerId: 'pl',
    walletId: 'w',
    roundId: 'r',
    gameId: 'g',
    kind: 'BET',
    money: { amount: '1.00', currency: 'BRL' },
  };

  test('canonical JSON sorts keys recursively and drops undefined', () => {
    expect(canonicalJson({ b: 1, a: { d: 'x', c: undefined, b: [2, { z: 1, y: 2 }] } })).toBe(
      '{"a":{"b":[2,{"y":2,"z":1}],"d":"x"},"b":1}',
    );
  });

  test('is independent of key order and sensitive to every business field', () => {
    const reordered = Object.fromEntries(Object.entries(fields).reverse()) as typeof fields;
    expect(wagerPayloadHash(reordered)).toBe(wagerPayloadHash(fields));
    expect(wagerPayloadHash(fields)).toMatch(/^[0-9a-f]{64}$/);
    for (const key of Object.keys(fields) as (keyof typeof fields)[]) {
      const changed = { ...fields, [key]: key === 'money' ? { amount: '1.01', currency: 'BRL' } : 'other' };
      expect(wagerPayloadHash(changed)).not.toBe(wagerPayloadHash(fields));
    }
    expect(wagerPayloadHash({ ...fields, referenceExternalTransactionId: 'x' })).not.toBe(wagerPayloadHash(fields));
  });
});

describe('Inbox / Outbox messages', () => {
  test('inbox markProcessed is idempotent and payload-aware', () => {
    const m = InboxMessage.receive({ messageId: 'm', consumerName: 'c', payloadHash: 'h', receivedAt: at });
    expect(m.isProcessed()).toBe(false);
    m.markProcessed(at);
    m.markProcessed(new Date(at.getTime() + 1));
    expect(m.processedAt).toEqual(at);
    expect(m.matchesPayload('h')).toBe(true);
    expect(m.matchesPayload('x')).toBe(false);
  });

  test('outbox retry uses capped exponential backoff; publication is final', () => {
    const { wallet, openingEntry } = Wallet.open({
      id: 'w',
      playerId: 'p',
      initialBalance: brl('1.00'),
      opening: { transactionId: 't', ledgerEntryId: 'l' },
      at,
    });
    const event = WalletBalanceChanged.from(wallet, openingEntry!, { eventId: 'e1', correlationId: 'c', occurredAt: at });
    const m = OutboxMessage.enqueue(event);
    expect(m.id).toBe('e1');
    expect(m.isDue(at)).toBe(true);
    const policy = { baseDelayMs: 1_000, maxDelayMs: 4_000 };
    const delays: number[] = [];
    for (let i = 0; i < 5; i++) {
      m.scheduleRetry(at, 'boom', policy, () => 1);
      delays.push(m.nextAttemptAt!.getTime() - at.getTime());
    }
    expect(delays).toEqual([1_000, 2_000, 4_000, 4_000, 4_000]);
    expect(m.attempts).toBe(5);
    expect(m.isDue(at)).toBe(false);
    m.markPublished(at);
    expect(m.isPending()).toBe(false);
    expect(m.isDue(new Date(at.getTime() + 1e9))).toBe(false);
    expect(m.payload).toMatchObject({ eventType: 'WalletBalanceChanged', version: 1, eventId: 'e1' });
  });

  test('pending-reference policy: exhaustion and growing delays', () => {
    const policy = new PendingReferencePolicy(4, 1_000, 5_000, () => 1);
    expect(policy.isExhausted(3)).toBe(false);
    expect(policy.isExhausted(4)).toBe(true);
    expect([1, 2, 3, 4].map((n) => policy.nextAttemptAt(n, at).getTime() - at.getTime())).toEqual([
      1_000, 2_000, 4_000, 5_000,
    ]);
  });
});
