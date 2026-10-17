import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money/money';
import { CurrencyMismatchError, InvalidMoneyError, MoneyOverflowError } from '../../src/domain/shared/domain-error';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money — parsing and scale', () => {
  test.each([
    ['25', '25.00'],
    ['25.5', '25.50'],
    ['25.50', '25.50'],
    ['0', '0.00'],
    ['0.01', '0.01'],
    ['999999999999999999.99', '999999999999999999.99'],
  ])('accepts %p and serializes with exactly 2 decimals as %p', (input, expected) => {
    expect(brl(input).amount).toBe(expected);
    expect(brl(input).toJSON()).toEqual({ amount: expected, currency: 'BRL' });
  });

  test.each([
    ['', 'empty'],
    ['NaN', 'NaN'],
    ['Infinity', 'Infinity'],
    ['-Infinity', 'negative infinity'],
    ['1e3', 'scientific notation'],
    ['1E-2', 'scientific notation (upper)'],
    ['10.005', 'more than 2 decimals (no silent rounding)'],
    ['0.001', 'sub-cent'],
    ['1.', 'trailing dot'],
    ['.5', 'leading dot'],
    ['+1.00', 'explicit plus'],
    [' 1.00', 'whitespace'],
    ['1,00', 'comma decimal separator'],
    ['01.00', 'leading zero'],
    ['0x10', 'hex'],
    ['1_000', 'separator'],
  ])('rejects %p (%s)', (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test('rejects negative values in input contracts', () => {
    expect(() => brl('-1.00')).toThrow(InvalidMoneyError);
    expect(Money.fromSigned({ amount: '-1.00', currency: 'BRL' }).amount).toBe('-1.00');
  });

  test('rejects non-string amounts (numbers are never accepted for money)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: null as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each(['brl', 'BR', 'BRLL', '', '12A'])('rejects invalid currency %p', (currency) => {
    expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
  });

  test('rejects values beyond numeric(20,2)', () => {
    expect(() => brl('1000000000000000000.00')).toThrow(MoneyOverflowError);
    expect(() => brl('999999999999999999.99').add(brl('0.01'))).toThrow(MoneyOverflowError);
  });
});

describe('Money — arithmetic is exact and immutable', () => {
  test('0.1 + 0.2 === 0.30 exactly (no floating point)', () => {
    expect(brl('0.10').add(brl('0.20')).equals(brl('0.30'))).toBe(true);
  });

  test('large values keep every cent', () => {
    expect(brl('900000000000000000.01').add(brl('99999999999999999.98')).amount).toBe('999999999999999999.99');
  });

  test('subtract / negate / sign predicates', () => {
    const diff = brl('10.00').subtract(brl('10.01'));
    expect(diff.amount).toBe('-0.01');
    expect(diff.isNegative()).toBe(true);
    expect(diff.negate().amount).toBe('0.01');
    expect(brl('0.00').isZero()).toBe(true);
    expect(Money.fromSigned({ amount: '-0.00', currency: 'BRL' }).isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
    expect(brl('1.00').isLessThan(brl('1.01'))).toBe(true);
    expect(brl('1.01').isLessThan(brl('1.01'))).toBe(false);
  });

  test('operations return new instances and never mutate', () => {
    const a = brl('10.00');
    const b = a.add(brl('5.00'));
    expect(a.amount).toBe('10.00');
    expect(b.amount).toBe('15.00');
    expect(b).not.toBe(a);
    expect(Object.isFrozen(a)).toBe(true);
  });

  test('zero()', () => {
    expect(Money.zero('USD').toJSON()).toEqual({ amount: '0.00', currency: 'USD' });
  });
});

describe('Money — currency conflicts', () => {
  const usd = Money.from({ amount: '1.00', currency: 'USD' });
  test.each([
    ['add', () => brl('1.00').add(usd)],
    ['subtract', () => brl('1.00').subtract(usd)],
    ['isLessThan', () => brl('1.00').isLessThan(usd)],
  ])('%s across currencies throws a domain error', (_, op) => {
    expect(op).toThrow(CurrencyMismatchError);
  });

  test('equals is false across currencies (same amount)', () => {
    expect(brl('1.00').equals(usd)).toBe(false);
  });
});
