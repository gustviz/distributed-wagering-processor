import type { MoneyProps } from '../../domain/money/money';
import type { FailureCode } from '../../domain/wagering/failure-code';
import type { WagerTransaction, WagerTransactionStatus } from '../../domain/wagering/wager-transaction';

/** Outcome of a submission. Replays return the ORIGINAL outcome (status + balance observed then). */
export interface TransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance?: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}

export function toTransactionResult(tx: WagerTransaction, idempotentReplay: boolean): TransactionResult {
  return {
    transactionId: tx.id,
    status: tx.status,
    ...(tx.observedBalance ? { balance: tx.observedBalance.toJSON() } : {}),
    ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    idempotentReplay,
  };
}

/** Full read model of a transaction (GET endpoints). */
export interface TransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  status: WagerTransactionStatus;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  observedBalance?: MoneyProps;
  referenceAttempts: number;
  nextAttemptAt?: string;
  createdAt: string;
  processedAt?: string;
}

export function toTransactionView(tx: WagerTransaction): TransactionView {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
    status: tx.status,
    ...(tx.referenceExternalTransactionId ? { referenceExternalTransactionId: tx.referenceExternalTransactionId } : {}),
    ...(tx.referenceTransactionId ? { referenceTransactionId: tx.referenceTransactionId } : {}),
    ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    ...(tx.observedBalance ? { observedBalance: tx.observedBalance.toJSON() } : {}),
    referenceAttempts: tx.referenceAttempts,
    ...(tx.nextAttemptAt ? { nextAttemptAt: tx.nextAttemptAt.toISOString() } : {}),
    createdAt: tx.createdAt.toISOString(),
    ...(tx.processedAt ? { processedAt: tx.processedAt.toISOString() } : {}),
  };
}
