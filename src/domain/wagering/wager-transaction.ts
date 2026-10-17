import { Money } from '../money/money';
import { InvalidTransactionError, InvalidTransactionStateError } from '../shared/domain-error';
import { invertDirection, LedgerDirection } from '../wallet/wallet-ledger-entry';
import { FailureCode } from './failure-code';

export enum WagerTransactionKind {
  /** Internal: opening credit of a wallet. Never accepted from API or queue. */
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export type SubmittableKind = Exclude<WagerTransactionKind, WagerTransactionKind.Opening>;

export const SUBMITTABLE_KINDS: readonly SubmittableKind[] = [
  WagerTransactionKind.Bet,
  WagerTransactionKind.Win,
  WagerTransactionKind.Loss,
  WagerTransactionKind.Refund,
  WagerTransactionKind.Rollback,
];

export enum WagerTransactionStatus {
  /** Accepted, not applied yet (in-memory only: processing is synchronous within one SQL transaction). */
  Pending = 'PENDING',
  /** Waiting for the referenced transaction; retried by the scheduled worker. */
  PendingReference = 'PENDING_REFERENCE',
  /** Applied. Terminal. */
  Processed = 'PROCESSED',
  /** Business-rule violation. Terminal. */
  Rejected = 'REJECTED',
  /** Permanent infrastructure/unexpected error. Terminal, auditable. */
  Failed = 'FAILED',
}

const TERMINAL: ReadonlySet<WagerTransactionStatus> = new Set([
  WagerTransactionStatus.Processed,
  WagerTransactionStatus.Rejected,
  WagerTransactionStatus.Failed,
]);

/**
 * Valid transitions. Everything else throws InvalidTransactionStateError.
 *
 *   PENDING ──────────► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
 *   PENDING_REFERENCE ─► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (another retry)
 *   PROCESSED, REJECTED, FAILED: terminal
 */
const TRANSITIONS: Readonly<Record<WagerTransactionStatus, readonly WagerTransactionStatus[]>> = {
  [WagerTransactionStatus.Pending]: [
    WagerTransactionStatus.Processed,
    WagerTransactionStatus.Rejected,
    WagerTransactionStatus.Failed,
    WagerTransactionStatus.PendingReference,
  ],
  [WagerTransactionStatus.PendingReference]: [
    WagerTransactionStatus.Processed,
    WagerTransactionStatus.Rejected,
    WagerTransactionStatus.Failed,
    WagerTransactionStatus.PendingReference,
  ],
  [WagerTransactionStatus.Processed]: [],
  [WagerTransactionStatus.Rejected]: [],
  [WagerTransactionStatus.Failed]: [],
};

/** Which kinds each kind may reference. Absent key = may not carry a reference. */
const ALLOWED_REFERENCE_KINDS: Partial<Record<WagerTransactionKind, readonly WagerTransactionKind[]>> = {
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [
    WagerTransactionKind.Bet,
    WagerTransactionKind.Win,
    WagerTransactionKind.Refund,
  ],
};

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: SubmittableKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  /** Wallet balance observed when the outcome was decided; returned on replays. */
  observedBalance?: Money;
  referenceAttempts: number;
  nextAttemptAt?: Date;
  updatedAt: Date;
}

export type ReferenceAssessment =
  | { readonly type: 'accept' }
  | { readonly type: 'wait' }
  | { readonly type: 'reject'; readonly code: FailureCode };

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** Provider-side id of the referenced transaction — not the internal id. */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId: string | undefined,
    private _failureCode: FailureCode | undefined,
    private _processedAt: Date | undefined,
    private _observedBalance: Money | undefined,
    private _referenceAttempts: number,
    private _nextAttemptAt: Date | undefined,
    private _updatedAt: Date,
  ) {}

  /** Born PENDING. Validates the reference requirement per kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if ((props.kind as WagerTransactionKind) === WagerTransactionKind.Opening) {
      throw new InvalidTransactionError('OPENING is internal and cannot be submitted');
    }
    for (const field of [
      'id',
      'providerId',
      'externalTransactionId',
      'idempotencyKey',
      'payloadHash',
      'walletId',
      'playerId',
      'roundId',
      'gameId',
    ] as const) {
      if (!props[field]) {
        throw new InvalidTransactionError(`${field} is required`);
      }
    }
    const reference = props.referenceExternalTransactionId;
    const allowed = ALLOWED_REFERENCE_KINDS[props.kind];
    if (reference !== undefined) {
      if (!allowed) {
        throw new InvalidTransactionError(`${props.kind} cannot reference another transaction`);
      }
      if (reference === '' || reference === props.externalTransactionId) {
        throw new InvalidTransactionError('referenceExternalTransactionId is invalid');
      }
    }
    const tx = WagerTransaction.newTransaction(props.kind, props, reference);
    if (tx.requiresReference() && reference === undefined) {
      throw new InvalidTransactionError(`${props.kind} requires referenceExternalTransactionId`);
    }
    if (props.kind === WagerTransactionKind.Loss ? props.money.isNegative() : !props.money.isPositive()) {
      throw new InvalidTransactionError(
        props.kind === WagerTransactionKind.Loss
          ? 'LOSS amount must not be negative'
          : `${props.kind} amount must be greater than zero`,
      );
    }
    return tx;
  }

  /** The internal OPENING credit of a new wallet. Processed immediately by the caller. */
  static createOpening(props: {
    id: string;
    walletId: string;
    playerId: string;
    money: Money;
    payloadHash: string;
    createdAt: Date;
  }): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new InvalidTransactionError('OPENING amount must be greater than zero');
    }
    return WagerTransaction.newTransaction(
      WagerTransactionKind.Opening,
      {
        ...props,
        providerId: OPENING_PROVIDER_ID,
        externalTransactionId: `opening:${props.walletId}`,
        idempotencyKey: `${OPENING_PROVIDER_ID}:opening:${props.walletId}`,
        roundId: 'opening',
        gameId: OPENING_PROVIDER_ID,
      },
      undefined,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.observedBalance,
      state.referenceAttempts,
      state.nextAttemptAt,
      state.updatedAt,
    );
  }

  private static newTransaction(
    kind: WagerTransactionKind,
    p: Omit<CreateWagerTransactionProps, 'kind' | 'referenceExternalTransactionId'>,
    reference: string | undefined,
  ): WagerTransaction {
    return new WagerTransaction(
      p.id,
      p.providerId,
      p.externalTransactionId,
      p.idempotencyKey,
      p.payloadHash,
      p.walletId,
      p.playerId,
      p.roundId,
      p.gameId,
      kind,
      p.money,
      reference,
      p.createdAt,
      WagerTransactionStatus.Pending,
      undefined,
      undefined,
      undefined,
      undefined,
      0,
      undefined,
      p.createdAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }
  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }
  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }
  get processedAt(): Date | undefined {
    return this._processedAt;
  }
  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }
  get referenceAttempts(): number {
    return this._referenceAttempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get updatedAt(): Date {
    return this._updatedAt;
  }

  // ---- transitions -------------------------------------------------------

  markProcessed(referenceTransactionId: string | undefined, at: Date, balanceAfter: Money): void {
    this.transitionTo(WagerTransactionStatus.Processed);
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._observedBalance = balanceAfter;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  /** Each call counts one resolution attempt and schedules the next one. */
  markPendingReference(nextAttemptAt: Date, at: Date, observedBalance: Money): void {
    this.transitionTo(WagerTransactionStatus.PendingReference);
    this._referenceAttempts += 1;
    this._nextAttemptAt = nextAttemptAt;
    this._observedBalance = observedBalance;
    this._updatedAt = at;
  }

  reject(code: FailureCode, at: Date, observedBalance: Money | undefined): void {
    this.transitionTo(WagerTransactionStatus.Rejected);
    this._failureCode = code;
    this._observedBalance = observedBalance;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  fail(code: FailureCode, at: Date): void {
    this.transitionTo(WagerTransactionStatus.Failed);
    this._failureCode = code;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  // ---- domain queries ----------------------------------------------------

  isTerminal(): boolean {
    return TERMINAL.has(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  isReversal(): boolean {
    return this.requiresReference();
  }

  hasReference(): boolean {
    return this.referenceExternalTransactionId !== undefined;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  /**
   * BET debits; WIN, REFUND and OPENING credit; ROLLBACK inverts its reference.
   * LOSS has no direction (it never touches the balance).
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback:
        if (!reference) {
          throw new InvalidTransactionError('ROLLBACK direction requires the referenced transaction');
        }
        return invertDirection(reference.ledgerDirectionFor());
      case WagerTransactionKind.Loss:
        throw new InvalidTransactionError('LOSS does not move the balance');
    }
  }

  /**
   * Validates a resolved reference. Ownership is checked first (another
   * player's/round's transaction is never acceptable), then kind, then the
   * reference's own state, then the amount for reversals.
   *
   * "Already reversed" needs a query, so the application service checks it
   * (and a partial unique index enforces it in the schema).
   */
  assessReference(reference: WagerTransaction): ReferenceAssessment {
    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.money.currency !== this.money.currency ||
      reference.roundId !== this.roundId
    ) {
      return { type: 'reject', code: FailureCode.ReferenceMismatch };
    }
    if (!ALLOWED_REFERENCE_KINDS[this.kind]?.includes(reference.kind)) {
      return { type: 'reject', code: FailureCode.ReferenceKindNotAllowed };
    }
    switch (reference.status) {
      case WagerTransactionStatus.Pending:
      case WagerTransactionStatus.PendingReference:
        return { type: 'wait' };
      case WagerTransactionStatus.Rejected:
      case WagerTransactionStatus.Failed:
        return { type: 'reject', code: FailureCode.ReferenceNotProcessed };
      case WagerTransactionStatus.Processed:
        break;
    }
    if (this.isReversal() && !reference.money.equals(this.money)) {
      return { type: 'reject', code: FailureCode.ReversalAmountMismatch };
    }
    return { type: 'accept' };
  }

  toState(): WagerTransactionState {
    return {
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash,
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind,
      money: this.money,
      referenceExternalTransactionId: this.referenceExternalTransactionId,
      createdAt: this.createdAt,
      status: this._status,
      referenceTransactionId: this._referenceTransactionId,
      failureCode: this._failureCode,
      processedAt: this._processedAt,
      observedBalance: this._observedBalance,
      referenceAttempts: this._referenceAttempts,
      nextAttemptAt: this._nextAttemptAt,
      updatedAt: this._updatedAt,
    };
  }

  private transitionTo(next: WagerTransactionStatus): void {
    if (!TRANSITIONS[this._status].includes(next)) {
      throw new InvalidTransactionStateError(this.id, this._status, next);
    }
    this._status = next;
  }
}

/** Reserved provider id for internal transactions (OPENING). Rejected at the API boundary. */
export const OPENING_PROVIDER_ID = 'internal';
