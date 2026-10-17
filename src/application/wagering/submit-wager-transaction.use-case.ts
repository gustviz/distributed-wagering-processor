import { InboxMessage } from '../../domain/messaging/inbox-message';
import type { Money } from '../../domain/money/money';
import { type SubmittableKind, WagerTransaction } from '../../domain/wagering/wager-transaction';
import {
  ExternalTransactionConflictError,
  IdempotencyConflictError,
  InboxPayloadConflictError,
  TransientInfrastructureError,
  UniqueViolationError,
  WalletNotFoundError,
} from '../errors';
import { wagerPayloadHash } from '../idempotency/payload-hash';
import type {
  AppLogger,
  Clock,
  FaultInjector,
  IdGenerator,
  Metrics,
  TransactionScope,
  TransactionSource,
  UnitOfWork,
} from '../ports';
import { toTransactionResult, type TransactionResult } from './transaction-result';
import type { WagerTransactionProcessor } from './wager-transaction-processor';

export interface SubmitWagerTransactionCommand {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: SubmittableKind;
  money: Money;
  referenceExternalTransactionId?: string;
  correlationId: string;
  source: TransactionSource;
  /** Present when the command comes from a queue: recorded in the same SQL transaction. */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

/** Constraints whose violation means "someone else committed this request first" → re-run as replay. */
const IDEMPOTENCY_CONSTRAINTS = new Set(['uq_wager_tx_idempotency_key', 'uq_wager_tx_provider_external']);
const MAX_ATTEMPTS = 4;

/**
 * Single entry point for wagering operations, used by BOTH the HTTP controller
 * and the SQS consumer.
 *
 * Concurrency: one SQL transaction per attempt. The wallet row is locked
 * (`SELECT ... FOR UPDATE`), so operations on the same wallet are serialized
 * while different wallets proceed in parallel. The idempotency lookup is
 * repeated after the lock is acquired, so a duplicate that waited for the lock
 * sees the committed original and replays it instead of applying twice.
 * Unique constraints are the final arbiter: if two duplicates still race (e.g.
 * a conflicting payload naming another wallet), the loser gets a unique
 * violation and the retry turns into a replay/conflict.
 */
export class SubmitWagerTransactionUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly processor: WagerTransactionProcessor,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly faults: FaultInjector,
  ) {}

  async execute(command: SubmitWagerTransactionCommand): Promise<TransactionResult> {
    const startedAt = performance.now();
    const payloadHash = wagerPayloadHash({ ...command, money: command.money.toJSON() });

    for (let attempt = 1; ; attempt++) {
      try {
        const result = await this.uow.transactional((scope) => this.attempt(scope, command, payloadHash));
        this.metrics.processingDuration(command.source, (performance.now() - startedAt) / 1000);
        if (result.idempotentReplay) {
          this.metrics.duplicateDetected('idempotent_replay');
        } else {
          this.metrics.transactionFinished(command.kind, result.status, command.source);
        }
        return result;
      } catch (error) {
        if (attempt >= MAX_ATTEMPTS || !this.isRetryable(error)) {
          throw error;
        }
        const reason = error instanceof UniqueViolationError ? error.constraint : 'contention';
        if (reason === 'contention') this.metrics.lockConflict();
        this.metrics.retry('submit_wager_transaction', reason);
        this.logger.warn('Retrying wager transaction', {
          attempt,
          reason,
          walletId: command.walletId,
          providerId: command.providerId,
        });
        await sleep(attempt * 10 + Math.random() * 40);
      }
    }
  }

  private async attempt(
    scope: TransactionScope,
    command: SubmitWagerTransactionCommand,
    payloadHash: string,
  ): Promise<TransactionResult> {
    const now = this.clock.now();

    // 1. Inbox: first statement, so a concurrent redelivery of the same message
    //    blocks on the unique key until we commit, then sees it as a duplicate.
    let inboxMessage: InboxMessage | undefined;
    if (command.inbox) {
      inboxMessage = InboxMessage.receive({ ...command.inbox, receivedAt: now });
      const existing = await scope.inbox.tryReceive(inboxMessage);
      if (existing) {
        if (!existing.matchesPayload(command.inbox.payloadHash)) {
          throw new InboxPayloadConflictError(command.inbox.messageId);
        }
        this.metrics.duplicateDetected('inbox_redelivery');
        const original = await scope.transactions.findExisting(
          command.idempotencyKey,
          command.providerId,
          command.externalTransactionId,
        );
        if (original) return this.replay(original, command, payloadHash);
        // Committed inbox rows always come with their transaction; this is defensive.
        throw new TransientInfrastructureError('Inbox row without transaction');
      }
    }

    // 2. Cheap replay before taking the wallet lock (also answers replays for unknown wallets).
    const early = await scope.transactions.findExisting(
      command.idempotencyKey,
      command.providerId,
      command.externalTransactionId,
    );
    if (early) return this.finishReplay(scope, inboxMessage, now, this.replay(early, command, payloadHash));

    // 3. Serialize on the wallet (row lock — never a global lock).
    const wallet = await scope.wallets.lockById(command.walletId);
    if (!wallet) throw new WalletNotFoundError(command.walletId);

    // 4. Re-check under the lock: a duplicate that waited for us now sees our commit.
    const existing = await scope.transactions.findExisting(
      command.idempotencyKey,
      command.providerId,
      command.externalTransactionId,
    );
    if (existing) return this.finishReplay(scope, inboxMessage, now, this.replay(existing, command, payloadHash));

    // 5. Apply.
    const tx = WagerTransaction.create({
      id: this.ids.next(),
      providerId: command.providerId,
      externalTransactionId: command.externalTransactionId,
      idempotencyKey: command.idempotencyKey,
      payloadHash,
      walletId: command.walletId,
      playerId: command.playerId,
      roundId: command.roundId,
      gameId: command.gameId,
      kind: command.kind,
      money: command.money,
      referenceExternalTransactionId: command.referenceExternalTransactionId,
      createdAt: now,
    });
    await this.processor.process(scope, wallet, tx, {
      now,
      correlationId: command.correlationId,
      causationId: command.inbox?.messageId,
      isNew: true,
    });

    if (inboxMessage) {
      inboxMessage.markProcessed(now);
      await scope.inbox.save(inboxMessage);
    }
    await this.faults.checkpoint('submit.before-commit');

    this.logger.info('Wager transaction decided', {
      transactionId: tx.id,
      walletId: tx.walletId,
      providerId: tx.providerId,
      kind: tx.kind,
      status: tx.status,
      failureCode: tx.failureCode,
    });
    return toTransactionResult(tx, false);
  }

  private replay(existing: WagerTransaction, command: SubmitWagerTransactionCommand, payloadHash: string): TransactionResult {
    if (existing.idempotencyKey !== command.idempotencyKey) {
      this.metrics.duplicateDetected('idempotency_conflict');
      throw new ExternalTransactionConflictError();
    }
    if (!existing.matchesPayload(payloadHash)) {
      this.metrics.duplicateDetected('idempotency_conflict');
      throw new IdempotencyConflictError(command.idempotencyKey);
    }
    return toTransactionResult(existing, true);
  }

  private async finishReplay(
    scope: TransactionScope,
    inboxMessage: InboxMessage | undefined,
    now: Date,
    result: TransactionResult,
  ): Promise<TransactionResult> {
    // A new message id carrying an already-applied operation is still "consumed".
    if (inboxMessage) {
      inboxMessage.markProcessed(now);
      await scope.inbox.save(inboxMessage);
    }
    return result;
  }

  private isRetryable(error: unknown): boolean {
    if (error instanceof UniqueViolationError) return IDEMPOTENCY_CONSTRAINTS.has(error.constraint);
    return error instanceof TransientInfrastructureError && error.contention;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
