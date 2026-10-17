import { Money } from '../money/money';
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidMoneyError,
} from '../shared/domain-error';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  /** Required when `initialBalance > 0`: the internal OPENING transaction. */
  opening?: { transactionId: string; ledgerEntryId: string };
  at: Date;
}

export interface WalletOpening {
  wallet: Wallet;
  /** CREDIT entry for the opening balance; absent when the wallet opens empty. */
  openingEntry?: WalletLedgerEntry;
}

export interface BalanceMovementProps {
  transactionId: string;
  ledgerEntryId: string;
  money: Money;
  at: Date;
}

/**
 * Wallet aggregate root.
 *
 * Every balance change goes through `debit`/`credit`, which return the ledger
 * entry describing it — so a balance change without a ledger entry is not
 * expressible through the model (the database enforces the same rule).
 *
 * `version` starts at 1 and increments only when the balance changes. The
 * persistence adapter uses it as an optimistic guard on top of the row lock.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): WalletOpening {
    const { initialBalance } = props;
    if (initialBalance.isNegative()) {
      throw new InvalidMoneyError('Initial balance must not be negative');
    }
    const wallet = new Wallet(
      props.id,
      props.playerId,
      initialBalance.currency,
      initialBalance,
      1,
      props.at,
      props.at,
    );
    if (initialBalance.isZero()) {
      return { wallet };
    }
    if (!props.opening) {
      throw new InvalidMoneyError('A positive initial balance requires an OPENING transaction');
    }
    const openingEntry = WalletLedgerEntry.create({
      id: props.opening.ledgerEntryId,
      walletId: wallet.id,
      transactionId: props.opening.transactionId,
      direction: LedgerDirection.Credit,
      money: initialBalance,
      balanceBefore: Money.zero(wallet.currency),
      balanceAfter: initialBalance,
      walletVersion: 1,
      createdAt: props.at,
    });
    return { wallet, openingEntry };
  }

  /** Reconstruction from persistence — does not revalidate transitions. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  debit(props: BalanceMovementProps): WalletLedgerEntry {
    this.assertMovement(props.money);
    if (!this.canDebit(props.money)) {
      throw new InsufficientFundsError(this.id);
    }
    return this.move(LedgerDirection.Debit, props);
  }

  credit(props: BalanceMovementProps): WalletLedgerEntry {
    this.assertMovement(props.money);
    return this.move(LedgerDirection.Credit, props);
  }

  apply(direction: LedgerDirection, props: BalanceMovementProps): WalletLedgerEntry {
    return direction === LedgerDirection.Debit ? this.debit(props) : this.credit(props);
  }

  toState(): WalletState {
    return {
      id: this.id,
      playerId: this.playerId,
      currency: this.currency,
      balance: this._balance,
      version: this._version,
      createdAt: this.createdAt,
      updatedAt: this._updatedAt,
    };
  }

  private move(direction: LedgerDirection, props: BalanceMovementProps): WalletLedgerEntry {
    const before = this._balance;
    const after =
      direction === LedgerDirection.Debit ? before.subtract(props.money) : before.add(props.money);
    const entry = WalletLedgerEntry.create({
      id: props.ledgerEntryId,
      walletId: this.id,
      transactionId: props.transactionId,
      direction,
      money: props.money,
      balanceBefore: before,
      balanceAfter: after,
      walletVersion: this._version + 1,
      createdAt: props.at,
    });
    // State changes only after the entry validated, so a failure leaves the aggregate untouched.
    this._balance = after;
    this._version += 1;
    this._updatedAt = props.at;
    return entry;
  }

  private assertMovement(money: Money): void {
    this.assertSameCurrency(money);
    if (!money.isPositive()) {
      throw new InvalidMoneyError('Movement amount must be positive');
    }
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
