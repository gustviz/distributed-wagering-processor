import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money/money';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidMoneyError,
  UnbalancedLedgerEntryError,
} from '../../src/domain/shared/domain-error';
import { Wallet } from '../../src/domain/wallet/wallet';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/wallet/wallet-ledger-entry';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-01-01T00:00:00Z');
let seq = 0;
const move = (amount: string) => ({ transactionId: `tx-${++seq}`, ledgerEntryId: `le-${seq}`, money: brl(amount), at });

function openWallet(initial = '100.00') {
  return Wallet.open({
    id: 'w-1',
    playerId: 'p-1',
    initialBalance: brl(initial),
    opening: { transactionId: 'tx-open', ledgerEntryId: 'le-open' },
    at,
  });
}

describe('Wallet', () => {
  test('opens at version 1 with an OPENING credit entry for a positive balance', () => {
    const { wallet, openingEntry } = openWallet('1000.00');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.amount).toBe('1000.00');
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.amount).toBe('0.00');
    expect(openingEntry?.balanceAfter.amount).toBe('1000.00');
    expect(openingEntry?.walletVersion).toBe(1);
  });

  test('opens empty without a ledger entry', () => {
    const { wallet, openingEntry } = Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('0.00'), at });
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test('a positive opening balance without an OPENING transaction is refused', () => {
    expect(() => Wallet.open({ id: 'w', playerId: 'p', initialBalance: brl('1.00'), at })).toThrow(InvalidMoneyError);
  });

  test('debit and credit move the balance, bump the version and return a balanced entry', () => {
    const { wallet } = openWallet('100.00');
    const debit = wallet.debit(move('80.00'));
    expect(wallet.balance.amount).toBe('20.00');
    expect(wallet.version).toBe(2);
    expect(debit.isBalanced()).toBe(true);
    expect(debit.balanceBefore.amount).toBe('100.00');
    expect(debit.balanceAfter.amount).toBe('20.00');
    expect(debit.walletVersion).toBe(2);

    const credit = wallet.credit(move('5.50'));
    expect(wallet.balance.amount).toBe('25.50');
    expect(wallet.version).toBe(3);
    expect(credit.walletVersion).toBe(3);
  });

  test('never goes negative: insufficient funds throws and leaves the aggregate untouched', () => {
    const { wallet } = openWallet('100.00');
    expect(wallet.canDebit(brl('100.01'))).toBe(false);
    expect(() => wallet.debit(move('100.01'))).toThrow(InsufficientFundsError);
    expect(wallet.balance.amount).toBe('100.00');
    expect(wallet.version).toBe(1);
  });

  test('can debit exactly the whole balance', () => {
    const { wallet } = openWallet('100.00');
    wallet.debit(move('100.00'));
    expect(wallet.balance.isZero()).toBe(true);
  });

  test('rejects movements in another currency', () => {
    const { wallet } = openWallet('100.00');
    const usd = { ...move('1.00'), money: Money.from({ amount: '1.00', currency: 'USD' }) };
    expect(() => wallet.credit(usd)).toThrow(CurrencyMismatchError);
    expect(() => wallet.debit(usd)).toThrow(CurrencyMismatchError);
    expect(() => wallet.canDebit(usd.money)).toThrow(CurrencyMismatchError);
  });

  test('rejects zero movements (a balance change must be a real change)', () => {
    const { wallet } = openWallet('100.00');
    expect(() => wallet.credit(move('0.00'))).toThrow(InvalidMoneyError);
  });

  test('rehydrate restores state without revalidating', () => {
    const wallet = Wallet.rehydrate({
      id: 'w',
      playerId: 'p',
      currency: 'BRL',
      balance: brl('7.00'),
      version: 42,
      createdAt: at,
      updatedAt: at,
    });
    expect(wallet.version).toBe(42);
    expect(wallet.balance.amount).toBe('7.00');
  });
});

describe('WalletLedgerEntry', () => {
  const base = {
    id: 'le',
    walletId: 'w',
    transactionId: 'tx',
    walletVersion: 2,
    createdAt: at,
  };

  test('create validates the arithmetic', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('100.00'),
        balanceAfter: brl('91.00'),
      }),
    ).toThrow(UnbalancedLedgerEntryError);
  });

  test('rejects non-positive amounts and negative balances', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Credit,
        money: brl('0.00'),
        balanceBefore: brl('1.00'),
        balanceAfter: brl('1.00'),
      }),
    ).toThrow(UnbalancedLedgerEntryError);
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        money: brl('2.00'),
        balanceBefore: brl('1.00'),
        balanceAfter: Money.fromSigned({ amount: '-1.00', currency: 'BRL' }),
      }),
    ).toThrow(UnbalancedLedgerEntryError);
  });

  test('is structurally immutable', () => {
    const entry = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      money: brl('1.00'),
      balanceBefore: brl('1.00'),
      balanceAfter: brl('2.00'),
    });
    expect(Object.isFrozen(entry)).toBe(true);
    expect(() => {
      (entry as unknown as { direction: string }).direction = 'DEBIT';
    }).toThrow();
    expect(entry.signedAmount().amount).toBe('1.00');
  });
});
