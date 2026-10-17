import { InvalidCursorError, TransactionNotFoundError, WalletNotFoundError } from '../errors';
import type { UnitOfWork } from '../ports';
import { toTransactionView, type TransactionView } from '../wagering/transaction-result';
import { type LedgerEntryView, toLedgerEntryView, toWalletView, type WalletView } from './wallet-view';

export interface LedgerPage {
  items: LedgerEntryView[];
  nextCursor: string | null;
}

export const MAX_LEDGER_PAGE_SIZE = 200;

/**
 * Read side. Ledger pagination is keyset-based on the per-wallet version
 * chain, which is dense, unique and immutable, so a cursor stays valid and
 * stable while new entries are appended. The cursor is opaque (base64url).
 */
export class WalletQueries {
  constructor(private readonly uow: UnitOfWork) {}

  async getWallet(walletId: string): Promise<WalletView> {
    const wallet = await this.uow.transactional((s) => s.wallets.findById(walletId), { readOnly: true });
    if (!wallet) throw new WalletNotFoundError(walletId);
    return toWalletView(wallet);
  }

  async getLedger(walletId: string, cursor: string | undefined, limit: number): Promise<LedgerPage> {
    const beforeVersion = cursor === undefined ? undefined : decodeCursor(cursor);
    return this.uow.transactional(
      async (s) => {
        const wallet = await s.wallets.findById(walletId);
        if (!wallet) throw new WalletNotFoundError(walletId);
        const entries = await s.ledger.listByWallet(walletId, { beforeVersion, limit: limit + 1 });
        const page = entries.slice(0, limit);
        const last = page.at(-1);
        return {
          items: page.map(toLedgerEntryView),
          nextCursor: entries.length > limit && last ? encodeCursor(last.walletVersion) : null,
        };
      },
      { readOnly: true },
    );
  }

  async getTransaction(transactionId: string): Promise<TransactionView> {
    const tx = await this.uow.transactional((s) => s.transactions.findById(transactionId), { readOnly: true });
    if (!tx) throw new TransactionNotFoundError();
    return toTransactionView(tx);
  }

  async getTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    const tx = await this.uow.transactional((s) => s.transactions.findByExternalId(providerId, externalTransactionId), {
      readOnly: true,
    });
    if (!tx) throw new TransactionNotFoundError();
    return toTransactionView(tx);
  }
}

export function encodeCursor(beforeVersion: number): string {
  return Buffer.from(JSON.stringify({ v: 1, before: beforeVersion })).toString('base64url');
}

export function decodeCursor(cursor: string): number {
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as { v?: unknown; before?: unknown };
    if (parsed.v === 1 && Number.isSafeInteger(parsed.before) && (parsed.before as number) > 0) {
      return parsed.before as number;
    }
  } catch {
    // fall through
  }
  throw new InvalidCursorError();
}
