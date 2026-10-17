import { createHash } from 'node:crypto';

export type CanonicalValue =
  | string
  | number
  | boolean
  | null
  | readonly CanonicalValue[]
  | { readonly [key: string]: CanonicalValue | undefined };

/**
 * Canonical JSON: object keys sorted lexicographically (by UTF-16 code unit),
 * no insignificant whitespace, `undefined` properties omitted. Arrays keep order.
 */
export function canonicalJson(value: CanonicalValue): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => canonicalJson(v)).join(',')}]`;
  }
  const record = value as { readonly [key: string]: CanonicalValue | undefined };
  const keys = Object.keys(record)
    .filter((k) => record[k] !== undefined)
    .sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson(record[k] as CanonicalValue)}`).join(',')}}`;
}

export function sha256Hex(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex');
}

/** Business fields that identify a wagering operation. Transport metadata never enters the hash. */
export interface WagerPayloadFields {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  /** Already normalized by `Money` (always 2 decimals), so "25.5" and "25.50" hash the same. */
  money: { amount: string; currency: string };
  referenceExternalTransactionId?: string;
}

/**
 * payloadHash = hex(SHA-256(canonicalJson(business fields))).
 * The Idempotency-Key header, the SQS envelope (messageId, occurredAt, type)
 * and any HTTP header are deliberately excluded.
 */
export function wagerPayloadHash(fields: WagerPayloadFields): string {
  return sha256Hex(
    canonicalJson({
      providerId: fields.providerId,
      externalTransactionId: fields.externalTransactionId,
      playerId: fields.playerId,
      walletId: fields.walletId,
      roundId: fields.roundId,
      gameId: fields.gameId,
      kind: fields.kind,
      money: { amount: fields.money.amount, currency: fields.money.currency },
      referenceExternalTransactionId: fields.referenceExternalTransactionId,
    }),
  );
}
