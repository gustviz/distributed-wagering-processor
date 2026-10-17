import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import {
  type BeforeApplicationShutdown,
  type DynamicModule,
  Inject,
  Injectable,
  Module,
  type OnApplicationBootstrap,
  type OnApplicationShutdown,
} from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import { RelayOutboxUseCase } from './application/messaging/relay-outbox.use-case';
import {
  APP_LOGGER,
  type AppLogger,
  CLOCK,
  type Clock,
  EVENT_PUBLISHER,
  type EventPublisher,
  FAULT_INJECTOR,
  type FaultInjector,
  HEALTH_PROBES,
  type HealthProbes,
  ID_GENERATOR,
  type IdGenerator,
  METRICS,
  type Metrics,
  UNIT_OF_WORK,
  type UnitOfWork,
} from './application/ports';
import { PENDING_REFERENCE_POLICY, PendingReferencePolicy } from './application/wagering/pending-reference-policy';
import { ResolvePendingReferencesUseCase } from './application/wagering/resolve-pending-references.use-case';
import { SubmitWagerTransactionUseCase } from './application/wagering/submit-wager-transaction.use-case';
import { WagerTransactionProcessor } from './application/wagering/wager-transaction-processor';
import { CreateWalletUseCase } from './application/wallet/create-wallet.use-case';
import { ReconcileWalletUseCase } from './application/wallet/reconcile-wallet.use-case';
import { WalletQueries } from './application/wallet/wallet-queries';
import { APP_CONFIG, type AppConfig } from './config/app-config';
import { createSqsClient, QueueUrls } from './infrastructure/messaging/sqs-client';
import { SqsEventPublisher } from './infrastructure/messaging/sqs-event-publisher';
import { JsonLogger } from './infrastructure/observability/logger';
import { PrometheusMetrics } from './infrastructure/observability/prometheus-metrics';
import { MikroOrmUnitOfWork } from './infrastructure/persistence/mikro-orm-unit-of-work';
import { ormConfig } from './infrastructure/persistence/mikro-orm.config';
import { EnvFaultInjector, SystemClock, UuidV7Generator } from './infrastructure/support';
import { AuthGuard, NoopProviderIdentity, PROVIDER_IDENTITY } from './interface/http/auth';
import {
  CREATE_WALLET,
  RECONCILE_WALLET,
  SUBMIT_WAGER,
  WALLET_QUERIES,
  WageringController,
  WalletsController,
} from './interface/http/controllers';
import { HttpExceptionFilter } from './interface/http/http-exception.filter';
import { HealthController, MetricsController, PROMETHEUS } from './interface/http/ops.controllers';
import { WagerTransactionsConsumer } from './interface/messaging/wager-transactions.consumer';
import { PeriodicTask } from './interface/workers/periodic-task';

const SQS_CLIENT = Symbol('SQSClient');
const QUEUE_URLS = Symbol('QueueUrls');
const PROCESSOR = Symbol('WagerTransactionProcessor');
const CONSUMER = Symbol('WagerTransactionsConsumer');
const OUTBOX_RELAY = Symbol('RelayOutboxUseCase');
const PENDING_RESOLVER = Symbol('ResolvePendingReferencesUseCase');

/**
 * Starts/stops the background roles according to config, so the same image
 * can run as API, consumer, relay, worker — or all of them (default).
 * Shutdown order: stop intake (consumer, workers) → Nest closes HTTP →
 * close the database pool and SQS client.
 */
@Injectable()
class RuntimeLifecycle implements OnApplicationBootstrap, BeforeApplicationShutdown, OnApplicationShutdown {
  private readonly tasks: PeriodicTask[] = [];

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    @Inject(APP_LOGGER) private readonly logger: AppLogger,
    @Inject(CONSUMER) private readonly consumer: WagerTransactionsConsumer,
    @Inject(OUTBOX_RELAY) private readonly relay: RelayOutboxUseCase,
    @Inject(PENDING_RESOLVER) private readonly resolver: ResolvePendingReferencesUseCase,
    @Inject(MikroORM) private readonly orm: MikroORM,
    @Inject(SQS_CLIENT) private readonly sqs: SQSClient,
  ) {}

  onApplicationBootstrap(): void {
    const { outbox, pendingReferences, consumer } = this.config;
    if (outbox.enabled) {
      this.tasks.push(
        new PeriodicTask(
          'outbox-relay',
          outbox.intervalMs,
          async () => ({ fullBatch: (await this.relay.runOnce(outbox.batchSize)) >= outbox.batchSize }),
          this.logger,
        ),
        new PeriodicTask(
          'outbox-stats',
          5_000,
          async () => {
            await this.relay.refreshStats();
            return { fullBatch: false };
          },
          this.logger,
        ),
      );
    }
    if (pendingReferences.enabled) {
      this.tasks.push(
        new PeriodicTask(
          'pending-references',
          pendingReferences.intervalMs,
          async () => ({
            fullBatch:
              (await this.resolver.runOnce({
                batchSize: pendingReferences.batchSize,
                leaseMs: pendingReferences.leaseMs,
              })) >= pendingReferences.batchSize,
          }),
          this.logger,
        ),
      );
    }
    for (const task of this.tasks) task.start();
    if (consumer.enabled) this.consumer.start();
  }

  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.logger.info('Shutting down', { signal });
    await Promise.all([this.consumer.stop(), ...this.tasks.map((t) => t.stop())]);
  }

  async onApplicationShutdown(): Promise<void> {
    await this.orm.close(true);
    this.sqs.destroy();
    this.logger.info('Shutdown complete');
  }
}

@Module({})
export class AppModule {
  static forRoot(config: AppConfig, logger: JsonLogger): DynamicModule {
    return {
      module: AppModule,
      controllers: [WalletsController, WageringController, HealthController, MetricsController],
      providers: [
        { provide: APP_CONFIG, useValue: config },
        { provide: APP_LOGGER, useValue: logger },
        { provide: PROMETHEUS, useValue: new PrometheusMetrics() },
        { provide: METRICS, useExisting: PROMETHEUS },
        { provide: CLOCK, useValue: new SystemClock() },
        { provide: ID_GENERATOR, useValue: new UuidV7Generator() },
        { provide: FAULT_INJECTOR, useValue: new EnvFaultInjector(config.faultInjection) },
        {
          provide: PENDING_REFERENCE_POLICY,
          useValue: new PendingReferencePolicy(
            config.pendingReferences.maxAttempts,
            config.pendingReferences.baseDelayMs,
            config.pendingReferences.maxDelayMs,
          ),
        },
        {
          provide: MikroORM,
          useFactory: () => MikroORM.init(ormConfig({ url: config.database.url, poolMax: config.database.poolMax })),
        },
        {
          provide: UNIT_OF_WORK,
          inject: [MikroORM],
          useFactory: (orm: MikroORM) =>
            new MikroOrmUnitOfWork(orm, {
              lockTimeoutMs: config.database.lockTimeoutMs,
              statementTimeoutMs: config.database.statementTimeoutMs,
            }),
        },
        { provide: SQS_CLIENT, useFactory: () => createSqsClient(config) },
        { provide: QUEUE_URLS, inject: [SQS_CLIENT], useFactory: (c: SQSClient) => new QueueUrls(c) },
        {
          provide: EVENT_PUBLISHER,
          inject: [SQS_CLIENT, QUEUE_URLS],
          useFactory: (c: SQSClient, urls: QueueUrls) => new SqsEventPublisher(c, urls, config.queues.events),
        },
        {
          provide: HEALTH_PROBES,
          inject: [MikroORM, SQS_CLIENT, QUEUE_URLS],
          useFactory: (orm: MikroORM, c: SQSClient, urls: QueueUrls): HealthProbes => ({
            database: async () => {
              await orm.em.getConnection().execute('SELECT 1');
            },
            queue: async () => {
              await c.send(
                new GetQueueAttributesCommand({
                  QueueUrl: await urls.get(config.queues.transactions),
                  AttributeNames: ['QueueArn'],
                }),
              );
            },
          }),
        },
        {
          provide: PROCESSOR,
          inject: [ID_GENERATOR, PENDING_REFERENCE_POLICY],
          useFactory: (ids: IdGenerator, policy: PendingReferencePolicy) => new WagerTransactionProcessor(ids, policy),
        },
        {
          provide: SUBMIT_WAGER,
          inject: [UNIT_OF_WORK, PROCESSOR, ID_GENERATOR, CLOCK, METRICS, APP_LOGGER, FAULT_INJECTOR],
          useFactory: (
            uow: UnitOfWork,
            p: WagerTransactionProcessor,
            ids: IdGenerator,
            clock: Clock,
            m: Metrics,
            l: AppLogger,
            f: FaultInjector,
          ) => new SubmitWagerTransactionUseCase(uow, p, ids, clock, m, l, f),
        },
        {
          provide: CREATE_WALLET,
          inject: [UNIT_OF_WORK, ID_GENERATOR, CLOCK, METRICS, APP_LOGGER],
          useFactory: (uow: UnitOfWork, ids: IdGenerator, clock: Clock, m: Metrics, l: AppLogger) =>
            new CreateWalletUseCase(uow, ids, clock, m, l),
        },
        { provide: WALLET_QUERIES, inject: [UNIT_OF_WORK], useFactory: (uow: UnitOfWork) => new WalletQueries(uow) },
        {
          provide: RECONCILE_WALLET,
          inject: [UNIT_OF_WORK, METRICS, APP_LOGGER, CLOCK],
          useFactory: (uow: UnitOfWork, m: Metrics, l: AppLogger, clock: Clock) =>
            new ReconcileWalletUseCase(uow, m, l, () => clock.now()),
        },
        {
          provide: OUTBOX_RELAY,
          inject: [UNIT_OF_WORK, EVENT_PUBLISHER, CLOCK, METRICS, APP_LOGGER, FAULT_INJECTOR],
          useFactory: (uow: UnitOfWork, pub: EventPublisher, clock: Clock, m: Metrics, l: AppLogger, f: FaultInjector) =>
            new RelayOutboxUseCase(uow, pub, clock, m, l, f, {
              baseDelayMs: config.outbox.retryBaseDelayMs,
              maxDelayMs: config.outbox.retryMaxDelayMs,
            }),
        },
        {
          provide: PENDING_RESOLVER,
          inject: [UNIT_OF_WORK, PROCESSOR, CLOCK, METRICS, APP_LOGGER],
          useFactory: (uow: UnitOfWork, p: WagerTransactionProcessor, clock: Clock, m: Metrics, l: AppLogger) =>
            new ResolvePendingReferencesUseCase(uow, p, clock, m, l),
        },
        {
          provide: CONSUMER,
          inject: [SQS_CLIENT, QUEUE_URLS, SUBMIT_WAGER, METRICS, APP_LOGGER, FAULT_INJECTOR],
          useFactory: (
            c: SQSClient,
            urls: QueueUrls,
            submit: SubmitWagerTransactionUseCase,
            m: Metrics,
            l: AppLogger,
            f: FaultInjector,
          ) => new WagerTransactionsConsumer(c, urls, submit, config, m, l, f),
        },
        { provide: PROVIDER_IDENTITY, useValue: new NoopProviderIdentity() },
        { provide: APP_GUARD, useClass: AuthGuard },
        { provide: APP_FILTER, useClass: HttpExceptionFilter },
        RuntimeLifecycle,
      ],
    };
  }
}
