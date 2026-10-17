import { z } from 'zod';
import type { SubmitWagerTransactionCommand } from '../../application/wagering/submit-wager-transaction.use-case';
import type { TransactionSource } from '../../application/ports';
import { Money } from '../../domain/money/money';
import { OPENING_PROVIDER_ID, SUBMITTABLE_KINDS, type SubmittableKind } from '../../domain/wagering/wager-transaction';

/**
 * Inbound contracts, shared by HTTP and SQS so both entry points accept
 * exactly the same payloads. Validation failures are contract errors
 * (HTTP 400 / SQS → DLQ), never business rejections.
 */
export class ContractValidationError extends Error {
  readonly code = 'INVALID_PAYLOAD';
  constructor(
    message: string,
    public readonly issues: { path: string; message: string }[] = [],
  ) {
    super(message);
    this.name = 'ContractValidationError';
  }
}

/** Printable ASCII without spaces, 1..128 chars: safe for keys, logs and SQS attributes. */
const identifier = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[\x21-\x7E]+$/, 'must be printable ASCII without spaces');

export const idempotencyKeySchema = z
  .string()
  .min(1)
  .max(255)
  .regex(/^[\x21-\x7E]+$/, 'must be printable ASCII without spaces');

export const uuidSchema = z.string().uuid();

/** Shape only; amount/currency rules are enforced by `Money.from` (single source of truth). */
const moneySchema = z.object({ amount: z.string(), currency: z.string() });

export const wagerPayloadSchema = z.object({
  providerId: identifier.refine((v) => v !== OPENING_PROVIDER_ID, 'is reserved'),
  externalTransactionId: identifier,
  playerId: identifier,
  walletId: uuidSchema,
  roundId: identifier,
  gameId: identifier,
  kind: z.enum(SUBMITTABLE_KINDS as [SubmittableKind, ...SubmittableKind[]]),
  money: moneySchema,
  referenceExternalTransactionId: identifier.optional(),
});

export type WagerPayload = z.infer<typeof wagerPayloadSchema>;

export const createWalletSchema = z.object({
  playerId: identifier,
  initialBalance: moneySchema,
});

export const wagerMessageSchema = z.object({
  messageId: z.string().min(1).max(256),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.string().datetime({ offset: true }),
  correlationId: z.string().min(1).max(128).optional(),
  data: wagerPayloadSchema.extend({ idempotencyKey: idempotencyKeySchema }),
});

export type WagerMessage = z.infer<typeof wagerMessageSchema>;

export function parseContract<S extends z.ZodTypeAny>(schema: S, input: unknown): z.infer<S> {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw new ContractValidationError(
      'Invalid payload',
      result.error.issues.map((i) => ({ path: i.path.join('.'), message: i.message })),
    );
  }
  return result.data;
}

/** Converts a validated payload into the use-case command (Money parsing may still reject). */
export function toSubmitCommand(
  payload: WagerPayload,
  meta: {
    idempotencyKey: string;
    correlationId: string;
    source: TransactionSource;
    inbox?: SubmitWagerTransactionCommand['inbox'];
  },
): SubmitWagerTransactionCommand {
  return {
    idempotencyKey: meta.idempotencyKey,
    providerId: payload.providerId,
    externalTransactionId: payload.externalTransactionId,
    playerId: payload.playerId,
    walletId: payload.walletId,
    roundId: payload.roundId,
    gameId: payload.gameId,
    kind: payload.kind,
    money: Money.from(payload.money),
    referenceExternalTransactionId: payload.referenceExternalTransactionId,
    correlationId: meta.correlationId,
    source: meta.source,
    inbox: meta.inbox,
  };
}
