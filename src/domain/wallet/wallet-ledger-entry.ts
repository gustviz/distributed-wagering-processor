import { Money } from '../money/money';
import { UnbalancedLedgerEntryError } from '../shared/domain-error';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export function invertDirection(direction: LedgerDirection): LedgerDirection {
  return direction === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  /** Wallet version produced by this movement; forms a gap-free chain per wallet. */
  walletVersion: number;
  createdAt: Date;
}

export type LedgerEntryState = CreateLedgerEntryProps;

/**
 * Immutable, append-only record of one balance movement.
 *
 * There are no mutable fields and no transition methods: immutability is
 * structural (readonly + frozen), and the database rejects UPDATE/DELETE too.
 */
export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly walletVersion: number,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  /** Validates the arithmetic: `balanceBefore ± money === balanceAfter`. */
  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const entry = WalletLedgerEntry.build(props);
    if (!entry.money.isPositive()) {
      throw new UnbalancedLedgerEntryError('Ledger entry amount must be positive');
    }
    if (entry.balanceBefore.isNegative() || entry.balanceAfter.isNegative()) {
      throw new UnbalancedLedgerEntryError('Ledger balances must not be negative');
    }
    if (!entry.isBalanced()) {
      throw new UnbalancedLedgerEntryError(
        `Unbalanced entry: ${entry.balanceBefore} ${entry.direction} ${entry.money} != ${entry.balanceAfter}`,
      );
    }
    if (!Number.isInteger(entry.walletVersion) || entry.walletVersion < 1) {
      throw new UnbalancedLedgerEntryError('Ledger entry wallet version must be a positive integer');
    }
    return entry;
  }

  /** Reconstruction from persistence — trusts already-persisted state. */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return WalletLedgerEntry.build(state);
  }

  isBalanced(): boolean {
    if (
      !this.money.hasSameCurrency(this.balanceBefore) ||
      !this.money.hasSameCurrency(this.balanceAfter)
    ) {
      return false;
    }
    const expected =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);
    return expected.equals(this.balanceAfter);
  }

  /** Signed effect on the balance: +money for credits, -money for debits. */
  signedAmount(): Money {
    return this.direction === LedgerDirection.Credit ? this.money : this.money.negate();
  }

  private static build(p: CreateLedgerEntryProps): WalletLedgerEntry {
    return new WalletLedgerEntry(
      p.id,
      p.walletId,
      p.transactionId,
      p.direction,
      p.money,
      p.balanceBefore,
      p.balanceAfter,
      p.walletVersion,
      p.createdAt,
    );
  }
}
