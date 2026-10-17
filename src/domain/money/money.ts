import { CurrencyMismatchError, InvalidMoneyError, MoneyOverflowError } from '../shared/domain-error';

/** Transport/persistence representation of an amount. */
export interface MoneyProps {
  /** Decimal string with up to 2 fractional digits, e.g. "25.00". Serialized with exactly 2. */
  amount: string;
  /** ISO-4217 alphabetic code, e.g. "BRL". */
  currency: string;
}

const SCALE = 2;
const FACTOR = 10n ** BigInt(SCALE);
/**
 * Mirrors the persistence column `numeric(20,2)`: at most 18 integer digits.
 * Rejecting out-of-range values here keeps the domain from producing amounts
 * the database would refuse (or worse, silently round).
 */
const MAX_ABS_MINOR = 10n ** 20n - 1n;
/**
 * Plain decimal notation only: optional sign, no leading zeros, up to 2 decimals.
 * Rejects "", "NaN", "Infinity", "1e3", "1.", ".5", "+1", " 1", "1.005", "01.00".
 */
const AMOUNT_PATTERN = /^(-)?(0|[1-9]\d*)(?:\.(\d{1,2}))?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;

/**
 * Immutable monetary value.
 *
 * Internally the amount is an integer number of minor units (`bigint`), so all
 * arithmetic is exact — there is no floating point anywhere in the money path.
 * The scale is fixed at 2 for every currency, as required by the contract.
 * Nothing is ever rounded: inputs with more precision are rejected instead.
 */
export class Money {
  private constructor(
    private readonly minor: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  /**
   * Parses an input contract value. Negative amounts are rejected: no inbound
   * contract (API, queue) may carry a negative amount.
   */
  static from(props: MoneyProps): Money {
    const money = Money.parse(props);
    if (money.isNegative()) {
      throw new InvalidMoneyError('Amount must not be negative');
    }
    return money;
  }

  /**
   * Parses a value that may legitimately be negative (e.g. a reconciliation
   * difference). Never use for inbound contracts.
   */
  static fromSigned(props: MoneyProps): Money {
    return Money.parse(props);
  }

  static zero(currency: string): Money {
    return new Money(0n, Money.assertCurrency(currency));
  }

  get amount(): string {
    const negative = this.minor < 0n;
    const abs = negative ? -this.minor : this.minor;
    const integer = abs / FACTOR;
    const fraction = (abs % FACTOR).toString().padStart(SCALE, '0');
    return `${negative ? '-' : ''}${integer.toString()}.${fraction}`;
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofMinor(this.minor + other.minor, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.ofMinor(this.minor - other.minor, this.currency);
  }

  negate(): Money {
    return Money.ofMinor(-this.minor, this.currency);
  }

  isZero(): boolean {
    return this.minor === 0n;
  }

  isPositive(): boolean {
    return this.minor > 0n;
  }

  isNegative(): boolean {
    return this.minor < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor < other.minor;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.minor === other.minor;
  }

  hasSameCurrency(other: Money): boolean {
    return this.currency === other.currency;
  }

  toJSON(): MoneyProps {
    return { amount: this.amount, currency: this.currency };
  }

  toString(): string {
    return `${this.amount} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }

  private static ofMinor(minor: bigint, currency: string): Money {
    if (minor > MAX_ABS_MINOR || minor < -MAX_ABS_MINOR) {
      throw new MoneyOverflowError();
    }
    return new Money(minor, currency);
  }

  private static parse(props: MoneyProps): Money {
    if (props === null || typeof props !== 'object') {
      throw new InvalidMoneyError('Money must be an object with amount and currency');
    }
    const { amount, currency } = props;
    if (typeof amount !== 'string') {
      throw new InvalidMoneyError('Amount must be a decimal string');
    }
    const match = AMOUNT_PATTERN.exec(amount);
    if (!match) {
      throw new InvalidMoneyError(
        `Invalid amount "${amount}": expected a plain decimal with at most ${SCALE} fractional digits`,
      );
    }
    const [, sign, integerPart, fractionPart = ''] = match;
    const minorAbs = BigInt(integerPart!) * FACTOR + BigInt(fractionPart.padEnd(SCALE, '0'));
    const minor = sign === '-' ? -minorAbs : minorAbs;
    if (minor > MAX_ABS_MINOR || minor < -MAX_ABS_MINOR) {
      throw new MoneyOverflowError();
    }
    // "-0.00" normalizes to zero; bigint has no negative zero.
    return new Money(minor, Money.assertCurrency(currency));
  }

  private static assertCurrency(currency: unknown): string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency "${String(currency)}": expected an ISO-4217 code`);
    }
    return currency;
  }
}
