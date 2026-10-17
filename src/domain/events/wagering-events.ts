import type { MoneyProps } from '../money/money';
import type { FailureCode } from '../wagering/failure-code';
import type { WagerTransaction, WagerTransactionKind } from '../wagering/wager-transaction';
import type { Wallet } from '../wallet/wallet';
import type { LedgerDirection, WalletLedgerEntry } from '../wallet/wallet-ledger-entry';
import { type EventContext, IntegrationEvent } from './integration-event';

/*
 * `aggregateId` is the walletId for every event: the wallet is the unit of
 * consistency and concurrency, so it is also the ordering key downstream
 * (SQS FIFO MessageGroupId). The transaction id travels in `data`.
 */

interface TransactionRef {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
}

function transactionRef(tx: WagerTransaction): TransactionRef {
  return {
    transactionId: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
  };
}

function envelope<T>(ctx: EventContext, aggregateId: string, data: T) {
  return {
    eventId: ctx.eventId,
    aggregateId,
    correlationId: ctx.correlationId,
    causationId: ctx.causationId,
    occurredAt: ctx.occurredAt,
    data,
  };
}

// ---------------------------------------------------------------------------

export interface WagerTransactionProcessedData extends TransactionRef {
  referenceTransactionId?: string;
  balanceAfter: MoneyProps;
  processedAt: string;
}

/** Any applied transaction, including LOSS (which does not move the balance). */
export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionProcessed {
    if (!tx.processedAt || !tx.observedBalance) {
      throw new Error(`Transaction ${tx.id} is not processed`);
    }
    return new WagerTransactionProcessed(
      envelope(ctx, tx.walletId, {
        ...transactionRef(tx),
        ...(tx.referenceTransactionId ? { referenceTransactionId: tx.referenceTransactionId } : {}),
        balanceAfter: tx.observedBalance.toJSON(),
        processedAt: tx.processedAt.toISOString(),
      }),
    );
  }
}

// ---------------------------------------------------------------------------

export interface WagerTransactionRejectedData extends TransactionRef {
  failureCode: FailureCode;
  referenceExternalTransactionId?: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionRejected {
    if (!tx.failureCode) {
      throw new Error(`Transaction ${tx.id} has no failure code`);
    }
    return new WagerTransactionRejected(
      envelope(ctx, tx.walletId, {
        ...transactionRef(tx),
        failureCode: tx.failureCode,
        ...(tx.referenceExternalTransactionId
          ? { referenceExternalTransactionId: tx.referenceExternalTransactionId }
          : {}),
      }),
    );
  }
}

// ---------------------------------------------------------------------------

export interface WagerTransactionPendingReferenceData extends TransactionRef {
  referenceExternalTransactionId: string;
  nextAttemptAt: string;
}

/** Raised once, when a transaction first starts waiting for its reference. */
export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionPendingReference {
    if (!tx.referenceExternalTransactionId || !tx.nextAttemptAt) {
      throw new Error(`Transaction ${tx.id} is not pending a reference`);
    }
    return new WagerTransactionPendingReference(
      envelope(ctx, tx.walletId, {
        ...transactionRef(tx),
        referenceExternalTransactionId: tx.referenceExternalTransactionId,
        nextAttemptAt: tx.nextAttemptAt.toISOString(),
      }),
    );
  }
}

// ---------------------------------------------------------------------------

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

/** Only raised when the balance actually changes. `walletVersion` lets consumers order/dedupe. */
export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged(
      envelope(ctx, wallet.id, {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      }),
    );
  }
}
