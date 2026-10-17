import type { EventContext, IntegrationEvent } from '../../domain/events/integration-event';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/events/wagering-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { FailureCode } from '../../domain/wagering/failure-code';
import {
  type WagerTransaction,
  WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction';
import type { Wallet } from '../../domain/wallet/wallet';
import { LedgerDirection, type WalletLedgerEntry } from '../../domain/wallet/wallet-ledger-entry';
import type { IdGenerator, TransactionScope } from '../ports';
import type { PendingReferencePolicy } from './pending-reference-policy';

interface Decision {
  events: IntegrationEvent<unknown>[];
  movement?: { entry: WalletLedgerEntry; expectedVersion: number };
}

export interface ProcessingContext {
  now: Date;
  correlationId: string;
  causationId?: string;
  /** true when the transaction row does not exist yet (insert), false for a retry (update). */
  isNew: boolean;
}

/**
 * Applies one wagering transaction to its (already locked) wallet and persists
 * every consequence in the caller's SQL transaction: transaction row, wallet
 * balance, ledger entry, outbox events and wake-up of dependents.
 *
 * Shared by the HTTP/SQS submit path and the pending-reference worker, so both
 * entry points follow exactly the same business rules.
 *
 * Precondition: the caller holds the wallet row lock (`FOR UPDATE`). Every
 * mutation of a wallet's transactions happens under that lock, which is what
 * makes the "already reversed" check and the balance check race-free.
 */
export class WagerTransactionProcessor {
  constructor(
    private readonly ids: IdGenerator,
    private readonly policy: PendingReferencePolicy,
  ) {}

  async process(scope: TransactionScope, wallet: Wallet, tx: WagerTransaction, ctx: ProcessingContext): Promise<void> {
    const decision = await this.decide(scope, wallet, tx, ctx);
    await this.persist(scope, wallet, tx, ctx, decision);
  }

  /** Reads what it needs and mutates the aggregates in memory; writes nothing. */
  private async decide(
    scope: TransactionScope,
    wallet: Wallet,
    tx: WagerTransaction,
    ctx: ProcessingContext,
  ): Promise<Decision> {
    if (tx.money.currency !== wallet.currency) {
      return this.reject(tx, wallet, FailureCode.CurrencyMismatch, ctx);
    }
    if (tx.playerId !== wallet.playerId) {
      return this.reject(tx, wallet, FailureCode.PlayerWalletMismatch, ctx);
    }

    let reference: WagerTransaction | undefined;
    if (tx.referenceExternalTransactionId !== undefined) {
      reference = await scope.transactions.findByExternalId(tx.providerId, tx.referenceExternalTransactionId);
      if (!reference) {
        return this.waitForReference(tx, wallet, FailureCode.ReferenceNotFound, ctx);
      }
      const assessment = tx.assessReference(reference);
      if (assessment.type === 'reject') {
        return this.reject(tx, wallet, assessment.code, ctx);
      }
      if (assessment.type === 'wait') {
        return this.waitForReference(tx, wallet, FailureCode.ReferenceNotProcessed, ctx);
      }
      if (tx.isReversal() && (await scope.transactions.hasProcessedReversalOf(reference.id))) {
        return this.reject(tx, wallet, FailureCode.ReferenceAlreadyReversed, ctx);
      }
    }

    if (!tx.affectsBalance()) {
      tx.markProcessed(reference?.id, ctx.now, wallet.balance);
      return { events: [WagerTransactionProcessed.from(tx, this.eventContext(ctx))] };
    }

    const direction = tx.ledgerDirectionFor(reference);
    if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
      // A BET without funds and a reversal that cannot be recovered are different
      // operational situations, hence different codes.
      return this.reject(
        tx,
        wallet,
        tx.isReversal() ? FailureCode.ReversalInsufficientFunds : FailureCode.InsufficientFunds,
        ctx,
      );
    }

    const expectedVersion = wallet.version;
    const entry = wallet.apply(direction, {
      transactionId: tx.id,
      ledgerEntryId: this.ids.next(),
      money: tx.money,
      at: ctx.now,
    });
    tx.markProcessed(reference?.id, ctx.now, wallet.balance);

    return {
      movement: { entry, expectedVersion },
      events: [
        WagerTransactionProcessed.from(tx, this.eventContext(ctx)),
        WalletBalanceChanged.from(wallet, entry, this.eventContext(ctx)),
      ],
    };
  }

  private reject(
    tx: WagerTransaction,
    wallet: Wallet,
    code: FailureCode,
    ctx: ProcessingContext,
  ): Decision {
    tx.reject(code, ctx.now, wallet.balance);
    return { events: [WagerTransactionRejected.from(tx, this.eventContext(ctx))] };
  }

  private waitForReference(
    tx: WagerTransaction,
    wallet: Wallet,
    exhaustedCode: FailureCode,
    ctx: ProcessingContext,
  ): Decision {
    const attemptsSoFar = tx.referenceAttempts + 1;
    if (this.policy.isExhausted(attemptsSoFar)) {
      return this.reject(tx, wallet, exhaustedCode, ctx);
    }
    const firstTime = tx.status !== WagerTransactionStatus.PendingReference;
    tx.markPendingReference(this.policy.nextAttemptAt(attemptsSoFar, ctx.now), ctx.now, wallet.balance);
    return { events: firstTime ? [WagerTransactionPendingReference.from(tx, this.eventContext(ctx))] : [] };
  }

  /** All writes of one outcome, in foreign-key order, inside the caller's SQL transaction. */
  private async persist(
    scope: TransactionScope,
    wallet: Wallet,
    tx: WagerTransaction,
    ctx: ProcessingContext,
    decision: Decision,
  ): Promise<void> {
    await this.saveTransaction(scope, tx, ctx);
    if (decision.movement) {
      await scope.wallets.saveBalance(wallet, decision.movement.expectedVersion);
      await scope.ledger.append(decision.movement.entry);
    }
    if (decision.events.length > 0) {
      await scope.outbox.add(decision.events.map((e) => OutboxMessage.enqueue(e)));
    }
    if (tx.isTerminal()) {
      // Anything waiting on this transaction can be decided now instead of at its next backoff.
      await scope.transactions.wakeDependents(tx.providerId, tx.externalTransactionId, ctx.now);
    }
  }

  private async saveTransaction(scope: TransactionScope, tx: WagerTransaction, ctx: ProcessingContext): Promise<void> {
    if (ctx.isNew) {
      await scope.transactions.insert(tx);
    } else {
      await scope.transactions.update(tx);
    }
  }

  private eventContext(ctx: ProcessingContext): EventContext {
    return {
      eventId: this.ids.next(),
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.now,
    };
  }
}
