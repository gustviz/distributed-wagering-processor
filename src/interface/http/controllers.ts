import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Res } from '@nestjs/common';
import type { Response } from 'express';
import type { SubmitWagerTransactionUseCase } from '../../application/wagering/submit-wager-transaction.use-case';
import type { TransactionResult } from '../../application/wagering/transaction-result';
import type { CreateWalletUseCase } from '../../application/wallet/create-wallet.use-case';
import type { ReconcileWalletUseCase } from '../../application/wallet/reconcile-wallet.use-case';
import { MAX_LEDGER_PAGE_SIZE, type WalletQueries } from '../../application/wallet/wallet-queries';
import { Money } from '../../domain/money/money';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { currentLogContext, runWithLogContext } from '../../infrastructure/observability/logger';
import {
  ContractValidationError,
  createWalletSchema,
  idempotencyKeySchema,
  parseContract,
  toSubmitCommand,
  uuidSchema,
  wagerPayloadSchema,
} from '../contracts/wager-contracts';

export const CREATE_WALLET = Symbol('CreateWalletUseCase');
export const SUBMIT_WAGER = Symbol('SubmitWagerTransactionUseCase');
export const WALLET_QUERIES = Symbol('WalletQueries');
export const RECONCILE_WALLET = Symbol('ReconcileWalletUseCase');

function correlationId(): string {
  return currentLogContext().correlationId ?? 'unknown';
}

function requireUuid(value: string, name: string): string {
  if (!uuidSchema.safeParse(value).success) {
    throw new ContractValidationError(`Invalid ${name}`, [{ path: name, message: 'must be a UUID' }]);
  }
  return value;
}

@Controller('wallets')
export class WalletsController {
  constructor(
    @Inject(CREATE_WALLET) private readonly createWallet: CreateWalletUseCase,
    @Inject(WALLET_QUERIES) private readonly queries: WalletQueries,
    @Inject(RECONCILE_WALLET) private readonly reconcile: ReconcileWalletUseCase,
  ) {}

  /** 201 created · 400 invalid payload · 409 wallet already exists for player+currency. */
  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown) {
    const input = parseContract(createWalletSchema, body);
    return this.createWallet.execute({
      playerId: input.playerId,
      initialBalance: Money.from(input.initialBalance),
      correlationId: correlationId(),
    });
  }

  @Get(':walletId')
  async get(@Param('walletId') walletId: string) {
    return this.queries.getWallet(requireUuid(walletId, 'walletId'));
  }

  /** Newest first. `cursor` is opaque; pass back `nextCursor` to continue. */
  @Get(':walletId/ledger')
  async ledger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limitRaw?: string,
  ) {
    const limit = limitRaw === undefined ? 50 : Number(limitRaw);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LEDGER_PAGE_SIZE) {
      throw new ContractValidationError('Invalid limit', [
        { path: 'limit', message: `must be an integer between 1 and ${MAX_LEDGER_PAGE_SIZE}` },
      ]);
    }
    return this.queries.getLedger(requireUuid(walletId, 'walletId'), cursor || undefined, limit);
  }

  /** Always 200 with `consistent` flag; divergences are logged + counted, never auto-fixed. */
  @Post(':walletId/reconciliation')
  @HttpCode(200)
  async reconciliation(@Param('walletId') walletId: string) {
    return this.reconcile.execute(requireUuid(walletId, 'walletId'));
  }
}

/**
 * Status mapping for submissions (identical for first call and replays):
 *   200 PROCESSED · 202 PENDING_REFERENCE (accepted, pending) · 422 REJECTED (business rule)
 *   500 FAILED (terminal infra failure) · 400 invalid · 409 idempotency conflict · 503 transient
 */
const STATUS_BY_RESULT: Record<WagerTransactionStatus, number> = {
  [WagerTransactionStatus.Processed]: 200,
  [WagerTransactionStatus.PendingReference]: 202,
  [WagerTransactionStatus.Pending]: 202,
  [WagerTransactionStatus.Rejected]: 422,
  [WagerTransactionStatus.Failed]: 500,
};

@Controller()
export class WageringController {
  constructor(
    @Inject(SUBMIT_WAGER) private readonly submit: SubmitWagerTransactionUseCase,
    @Inject(WALLET_QUERIES) private readonly queries: WalletQueries,
  ) {}

  @Post('wagering/transactions')
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKeyHeader: string | undefined,
    @Body() body: unknown,
    @Res({ passthrough: true }) response: Response,
  ): Promise<TransactionResult> {
    const keyCheck = idempotencyKeySchema.safeParse(idempotencyKeyHeader);
    if (!keyCheck.success) {
      throw new ContractValidationError('Idempotency-Key header is required', [
        { path: 'Idempotency-Key', message: 'required, 1..255 printable ASCII characters' },
      ]);
    }
    const payload = parseContract(wagerPayloadSchema, body);
    const result = await runWithLogContext({ walletId: payload.walletId, providerId: payload.providerId }, () =>
      this.submit.execute(
        toSubmitCommand(payload, { idempotencyKey: keyCheck.data, correlationId: correlationId(), source: 'http' }),
      ),
    );
    response.status(STATUS_BY_RESULT[result.status]);
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  async getTransaction(@Param('transactionId') transactionId: string) {
    return this.queries.getTransaction(requireUuid(transactionId, 'transactionId'));
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    return this.queries.getTransactionByExternalId(providerId, externalTransactionId);
  }
}
