import { FailureCode } from '../../domain/wagering/failure-code';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { TransientInfrastructureError } from '../errors';
import type { AppLogger, Clock, Metrics, PendingReferenceClaim, UnitOfWork } from '../ports';
import type { WagerTransactionProcessor } from './wager-transaction-processor';

export interface ResolvePendingReferencesOptions {
  batchSize: number;
  /** How long a claimed row is hidden from other workers while this one handles it. */
  leaseMs: number;
}

/**
 * One tick of the scheduled worker that retries PENDING_REFERENCE transactions.
 *
 * Multi-instance safety:
 *  - claiming uses `FOR UPDATE SKIP LOCKED` and pushes `next_attempt_at` forward
 *    (a lease), so instances do not grab the same rows;
 *  - each resolution runs in its own SQL transaction that locks the wallet and
 *    then re-reads the transaction, so even a double claim (expired lease) is
 *    harmless — the second one sees a non-pending status and skips.
 *  - if the process dies mid-way, the lease expires and another instance retries.
 */
export class ResolvePendingReferencesUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly processor: WagerTransactionProcessor,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async runOnce(options: ResolvePendingReferencesOptions): Promise<number> {
    const claims = await this.uow.transactional((scope) =>
      scope.transactions.claimDuePendingReferences(this.clock.now(), options.batchSize, options.leaseMs),
    );
    for (const claim of claims) {
      await this.resolve(claim);
    }
    return claims.length;
  }

  async resolve(claim: PendingReferenceClaim): Promise<void> {
    try {
      const outcome = await this.uow.transactional(async (scope) => {
        const wallet = await scope.wallets.lockById(claim.walletId);
        const tx = await scope.transactions.lockById(claim.transactionId);
        if (!wallet || !tx || tx.status !== WagerTransactionStatus.PendingReference) {
          return 'skipped';
        }
        await this.processor.process(scope, wallet, tx, {
          now: this.clock.now(),
          correlationId: tx.id,
          causationId: tx.id,
          isNew: false,
        });
        if (tx.isTerminal()) this.metrics.transactionFinished(tx.kind, tx.status, 'worker');
        this.logger.info('Pending reference attempt', {
          transactionId: tx.id,
          walletId: tx.walletId,
          providerId: tx.providerId,
          status: tx.status,
          attempts: tx.referenceAttempts,
          failureCode: tx.failureCode,
        });
        return tx.status;
      });
      this.metrics.pendingReferenceResolution(outcome);
    } catch (error) {
      if (error instanceof TransientInfrastructureError) {
        // Lease expires and the row is retried later; nothing to persist.
        this.metrics.retry('resolve_pending_reference', error.contention ? 'contention' : 'transient');
        this.logger.warn('Transient error resolving pending reference', {
          transactionId: claim.transactionId,
          walletId: claim.walletId,
        });
        return;
      }
      await this.markFailed(claim, error);
    }
  }

  /** A non-transient error would fail forever: park the transaction as FAILED (terminal, auditable). */
  private async markFailed(claim: PendingReferenceClaim, cause: unknown): Promise<void> {
    this.logger.error('Unexpected error resolving pending reference; marking FAILED', {
      transactionId: claim.transactionId,
      walletId: claim.walletId,
    }, cause);
    try {
      await this.uow.transactional(async (scope) => {
        await scope.wallets.lockById(claim.walletId);
        const tx = await scope.transactions.lockById(claim.transactionId);
        if (!tx || tx.isTerminal()) return;
        const now = this.clock.now();
        tx.fail(FailureCode.UnexpectedError, now);
        await scope.transactions.update(tx);
        // Like any terminal outcome: dependents can be decided now (REFERENCE_NOT_PROCESSED).
        await scope.transactions.wakeDependents(tx.providerId, tx.externalTransactionId, now);
        this.metrics.transactionFinished(tx.kind, tx.status, 'worker');
      });
      this.metrics.pendingReferenceResolution(WagerTransactionStatus.Failed);
    } catch (error) {
      this.logger.error('Could not mark transaction FAILED; will retry after lease', {
        transactionId: claim.transactionId,
      }, error);
    }
  }
}
