import { WagerTransactionProcessed, WalletBalanceChanged } from '../../domain/events/wagering-events';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import type { Money } from '../../domain/money/money';
import { WagerTransaction } from '../../domain/wagering/wager-transaction';
import { Wallet } from '../../domain/wallet/wallet';
import { UniqueViolationError, WalletAlreadyExistsError } from '../errors';
import { canonicalJson, sha256Hex } from '../idempotency/payload-hash';
import type { AppLogger, Clock, IdGenerator, Metrics, UnitOfWork } from '../ports';
import { toWalletView, type WalletView } from './wallet-view';

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: Money;
  correlationId: string;
}

/**
 * Opens a wallet. A positive initial balance becomes an internal OPENING
 * transaction with its CREDIT ledger entry — in the same SQL transaction —
 * so the ledger reconstructs the balance from the very first cent.
 */
export class CreateWalletUseCase {
  constructor(
    private readonly uow: UnitOfWork,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: Metrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(command: CreateWalletCommand): Promise<WalletView> {
    const now = this.clock.now();
    const walletId = this.ids.next();
    const openingTx = command.initialBalance.isPositive()
      ? WagerTransaction.createOpening({
          id: this.ids.next(),
          walletId,
          playerId: command.playerId,
          money: command.initialBalance,
          payloadHash: sha256Hex(
            canonicalJson({ kind: 'OPENING', walletId, playerId: command.playerId, money: { ...command.initialBalance.toJSON() } }),
          ),
          createdAt: now,
        })
      : undefined;

    const { wallet, openingEntry } = Wallet.open({
      id: walletId,
      playerId: command.playerId,
      initialBalance: command.initialBalance,
      opening: openingTx ? { transactionId: openingTx.id, ledgerEntryId: this.ids.next() } : undefined,
      at: now,
    });

    try {
      await this.uow.transactional(async (scope) => {
        await scope.wallets.insert(wallet);
        if (openingTx && openingEntry) {
          openingTx.markProcessed(undefined, now, wallet.balance);
          await scope.transactions.insert(openingTx);
          await scope.ledger.append(openingEntry);
          const ctx = () => ({ eventId: this.ids.next(), correlationId: command.correlationId, occurredAt: now });
          await scope.outbox.add([
            OutboxMessage.enqueue(WagerTransactionProcessed.from(openingTx, ctx())),
            OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, openingEntry, ctx())),
          ]);
        }
      });
    } catch (error) {
      if (error instanceof UniqueViolationError && error.constraint === 'uq_wallets_player_currency') {
        throw new WalletAlreadyExistsError();
      }
      throw error;
    }

    if (openingTx) this.metrics.transactionFinished(openingTx.kind, openingTx.status, 'internal');
    this.logger.info('Wallet opened', { walletId: wallet.id });
    return toWalletView(wallet);
  }
}
