import type { MoneyProps } from '../../domain/money/money';
import { WalletNotFoundError } from '../errors';
import type { AppLogger, Metrics, UnitOfWork } from '../ports';

export interface ReconciliationReport {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
  walletVersion: number;
  checkedAt: string;
}

/**
 * Compares the materialized balance with the balance rebuilt from the ledger.
 *
 * Runs in a REPEATABLE READ, read-only transaction: wallet and ledger are read
 * from the same snapshot without blocking writers. Divergences are never
 * "fixed": they are logged at error level, counted in a metric and reported.
 */
export class ReconcileWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
    private readonly now: () => Date = () => new Date(),
  ) {}

  async execute(walletId: string): Promise<ReconciliationReport> {
    const report = await this.uow.transactional(
      async (scope) => {
        const wallet = await scope.wallets.findById(walletId);
        if (!wallet) throw new WalletNotFoundError(walletId);
        const summary = await scope.ledger.summarize(walletId, wallet.currency);
        const calculated = summary.credits.subtract(summary.debits);
        const difference = wallet.balance.subtract(calculated);
        return {
          walletId,
          storedBalance: wallet.balance.toJSON(),
          calculatedBalance: calculated.toJSON(),
          difference: difference.toJSON(),
          consistent: difference.isZero(),
          checkedEntries: summary.entries,
          walletVersion: wallet.version,
          checkedAt: this.now().toISOString(),
        };
      },
      { isolation: 'repeatable read', readOnly: true },
    );

    this.metrics.reconciliation(report.consistent);
    if (!report.consistent) {
      // Identifiers only; the amounts are in the response, not in the logs.
      this.logger.error('Wallet reconciliation mismatch', {
        walletId,
        checkedEntries: report.checkedEntries,
        walletVersion: report.walletVersion,
      });
    }
    return report;
  }
}
