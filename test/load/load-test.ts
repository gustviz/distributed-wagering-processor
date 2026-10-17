/**
 * Load test — `bun run test:load`
 *
 * Drives the HTTP API of one or more running instances with a realistic mix of
 * wagering operations, including a hot wallet and idempotent resends, then
 * verifies that every wallet still reconciles with its ledger.
 *
 * Env:
 *   TARGETS        comma-separated base URLs (default http://localhost:3000)
 *   METRICS_TARGETS  instances to scrape /metrics from (default TARGETS). Behind a load
 *                  balancer each scrape reaches ONE replica, so server counters are a
 *                  sample; list every instance here for exact totals.
 *   DURATION_S     measured duration (default 30)
 *   CONCURRENCY    concurrent virtual providers (default 64)
 *   WALLETS        number of wallets (default 50)
 *   HOT_SHARE      share of traffic hitting wallet #0 (default 0.2)
 *   AWS_ENDPOINT_URL / SQS_EVENTS_QUEUE  events queue drained by a simulated downstream
 *                  consumer (default http://localhost:4566 / wager-events.fifo); set
 *                  EVENTS_SINK=off to skip the delivery check (reported as not verified)
 *   LOAD_DATABASE_URL  read access to the service database, used to list THIS run's
 *                  event ids (default postgres://postgres:postgres@localhost:5433/wagering)
 *   DRAIN_TIMEOUT_S / DELIVERY_TIMEOUT_S  how long to wait for the outbox to publish and
 *                  for the sink to receive every event of the run (default 60 / 60)
 *
 * The run FAILS (exit 1) when, after the traffic stops:
 *   - any wallet of the run does not reconcile;
 *   - any outbox event of the run is still unpublished after DRAIN_TIMEOUT_S;
 *   - any event of the run (identified by eventId) never reached the sink;
 *   - any request failed with a network error or HTTP 5xx.
 * The run's events are exactly the outbox rows whose aggregate is one of the wallets
 * this run created, so other activity on the same stack cannot mask a loss.
 *
 * Output: a report on stdout and test/load/results/<runId>.json
 */
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DeleteMessageBatchCommand, GetQueueUrlCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import pg from 'pg';

const targets = (process.env.TARGETS ?? 'http://localhost:3000').split(',').map((t) => t.trim().replace(/\/$/, ''));
const durationS = Number(process.env.DURATION_S ?? 30);
const concurrency = Number(process.env.CONCURRENCY ?? 64);
const walletCount = Number(process.env.WALLETS ?? 50);
const hotShare = Number(process.env.HOT_SHARE ?? 0.2);
const metricsTargets = (process.env.METRICS_TARGETS ?? targets.join(','))
  .split(',')
  .map((t) => t.trim().replace(/\/$/, ''));
const runId = `load-${Date.now().toString(36)}`;
const drainTimeoutS = Number(process.env.DRAIN_TIMEOUT_S ?? 60);
const deliveryTimeoutS = Number(process.env.DELIVERY_TIMEOUT_S ?? 60);
const db = new pg.Pool({
  connectionString: process.env.LOAD_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5433/wagering',
  max: 2,
});

/**
 * Simulated downstream consumer of the integration events: keeps the events
 * queue short (as in production) and records every eventId it receives, so the
 * run can prove delivery of its own events and count duplicates.
 */
class EventsSink {
  readonly seen = new Map<string, number>();
  private stopped = false;
  private loops: Promise<void>[] = [];
  private readonly client = new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: { accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test', secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test' },
  });

  async start(parallelism = 4): Promise<void> {
    const queueName = process.env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo';
    const url = (await this.client.send(new GetQueueUrlCommand({ QueueName: queueName }))).QueueUrl!;
    this.loops = Array.from({ length: parallelism }, () => this.loop(url));
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await Promise.all(this.loops);
  }

  private async loop(url: string): Promise<void> {
    while (!this.stopped) {
      try {
        const r = await this.client.send(
          new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['eventId'] }),
        );
        const messages = r.Messages ?? [];
        if (messages.length === 0) continue;
        for (const m of messages) {
          const id = m.MessageAttributes?.eventId?.StringValue ?? '';
          this.seen.set(id, (this.seen.get(id) ?? 0) + 1);
        }
        await this.client.send(
          new DeleteMessageBatchCommand({
            QueueUrl: url,
            Entries: messages.map((m, i) => ({ Id: `d${i}`, ReceiptHandle: m.ReceiptHandle })),
          }),
        );
      } catch {
        await Bun.sleep(200);
      }
    }
  }
}

interface Sample {
  ms: number;
  status: number;
  kind: string;
  replay: boolean;
}

const samples: Sample[] = [];
const networkErrors: string[] = [];
let rr = 0;
const target = () => targets[rr++ % targets.length]!;

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const r = await fetch(`${target()}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
  return { status: r.status, body: (await r.json().catch(() => ({}))) as Record<string, any> };
}

/** Gauges describe the shared database (every instance reports the same value): take the max, not the sum. */
const GAUGES = new Set(['wagering_outbox_lag_seconds', 'wagering_outbox_pending']);

async function scrape(): Promise<Map<string, number>> {
  // Counters are per instance (each has its own registry): sum them.
  const totals = new Map<string, number>();
  for (const t of metricsTargets) {
    try {
      const text = await (await fetch(`${t}/metrics`)).text();
      for (const line of text.split('\n')) {
        if (!line || line.startsWith('#')) continue;
        const [name, raw] = line.split(/\s+(?=[^\s]+$)/);
        if (!name || !raw) continue;
        const value = Number(raw);
        totals.set(name, GAUGES.has(name) ? Math.max(totals.get(name) ?? 0, value) : (totals.get(name) ?? 0) + value);
      }
    } catch {
      // instance unreachable: ignore for metrics
    }
  }
  return totals;
}

const sumMatching = (m: Map<string, number>, prefix: string) =>
  [...m].filter(([k]) => k.startsWith(prefix)).reduce((n, [, v]) => n + v, 0);

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))]!;
}

async function main() {
  console.log(`[load] targets=${targets.join(',')} duration=${durationS}s concurrency=${concurrency} wallets=${walletCount}`);
  await db.query('SELECT 1'); // fail fast if the run's events cannot be verified
  // Started before the wallets exist, so the OPENING events are part of the run too.
  const sink = process.env.EVENTS_SINK === 'off' ? undefined : new EventsSink();
  await sink?.start();
  const wallets: { walletId: string; playerId: string }[] = [];
  for (let i = 0; i < walletCount; i++) {
    const playerId = `${runId}-p${i}`;
    const r = await post('/wallets', { playerId, initialBalance: { amount: '100000.00', currency: 'BRL' } });
    if (r.status !== 201) throw new Error(`wallet creation failed: ${r.status} ${JSON.stringify(r.body)}`);
    wallets.push({ walletId: r.body.id, playerId });
  }

  const before = await scrape();
  const sent: { payload: Record<string, unknown>; key: string }[] = [];
  let seq = 0;

  const deadline = Date.now() + durationS * 1000;
  const startedAt = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (Date.now() < deadline) {
        let payload: Record<string, unknown>;
        let key: string;
        let kind: string;
        let replay = false;
        const roll = Math.random();
        if (roll < 0.1 && sent.length > 0) {
          ({ payload, key } = sent[Math.floor(Math.random() * sent.length)]!); // provider resend
          kind = String(payload.kind);
          replay = true;
        } else {
          const w = Math.random() < hotShare ? wallets[0]! : wallets[1 + Math.floor(Math.random() * (walletCount - 1))]!;
          kind = roll < 0.7 ? 'BET' : roll < 0.9 ? 'WIN' : 'LOSS';
          const ext = `${runId}-${++seq}`;
          payload = {
            providerId: 'load',
            externalTransactionId: ext,
            playerId: w.playerId,
            walletId: w.walletId,
            roundId: `r-${seq}`,
            gameId: 'load-game',
            kind,
            money: { amount: kind === 'LOSS' ? '0.00' : kind === 'WIN' ? '1.50' : '1.00', currency: 'BRL' },
          };
          key = `load:${ext}`;
          sent.push({ payload, key });
        }
        const t0 = performance.now();
        try {
          const r = await post('/wagering/transactions', payload, { 'Idempotency-Key': key });
          samples.push({ ms: performance.now() - t0, status: r.status, kind, replay });
        } catch (error) {
          networkErrors.push(String(error));
          samples.push({ ms: performance.now() - t0, status: 0, kind, replay });
        }
      }
    }),
  );
  const elapsedS = (performance.now() - startedAt) / 1000;
  const walletIds = wallets.map((w) => w.walletId);
  const failures: string[] = [];

  // 1. Outbox drain: every event of THIS run must be published within the timeout.
  const trafficEnded = performance.now();
  let pending: number;
  for (;;) {
    const r = await db.query<{ pending: number }>(
      'SELECT count(*)::int AS pending FROM outbox_messages WHERE aggregate_id = ANY($1::text[]) AND published_at IS NULL',
      [walletIds],
    );
    pending = r.rows[0]!.pending;
    if (pending === 0 || (performance.now() - trafficEnded) / 1000 >= drainTimeoutS) break;
    await Bun.sleep(500);
  }
  const drainedAfterS = Number(((performance.now() - trafficEnded) / 1000).toFixed(1));
  if (pending > 0) failures.push(`${pending} outbox events of this run still unpublished after ${drainTimeoutS}s`);

  // 2. Delivery: every event id of this run must reach the downstream consumer.
  const events = await db.query<{ id: string; lag_s: number | null }>(
    `SELECT id::text, EXTRACT(EPOCH FROM (published_at - occurred_at))::float8 AS lag_s
       FROM outbox_messages WHERE aggregate_id = ANY($1::text[])`,
    [walletIds],
  );
  const runEventIds = events.rows.map((e) => e.id);
  const runEventSet = new Set(runEventIds);
  const lags = events.rows
    .map((e) => e.lag_s)
    .filter((l): l is number => l !== null)
    .sort((a, b) => a - b);
  let missing: string[] = [];
  if (sink) {
    const deliveryDeadline = performance.now() + deliveryTimeoutS * 1000;
    for (;;) {
      missing = runEventIds.filter((id) => !sink.seen.has(id));
      if (missing.length === 0 || performance.now() >= deliveryDeadline) break;
      await Bun.sleep(500);
    }
    await sink.stop();
    if (missing.length > 0) {
      failures.push(
        `${missing.length} of ${runEventIds.length} events of this run were never delivered (e.g. ${missing.slice(0, 3).join(', ')})`,
      );
    }
  }
  const duplicateDeliveries = sink
    ? [...sink.seen].filter(([id, n]) => runEventSet.has(id) && n > 1).reduce((total, [, n]) => total + n - 1, 0)
    : 0;

  // 3. Every wallet of the run reconciles.
  const after = await scrape();
  const inconsistent: string[] = [];
  for (const w of wallets) {
    const r = await post(`/wallets/${w.walletId}/reconciliation`, {});
    if (!r.body.consistent) inconsistent.push(w.walletId);
  }
  if (inconsistent.length > 0) failures.push(`${inconsistent.length} wallets failed reconciliation`);

  const latencies = samples.map((s) => s.ms).sort((a, b) => a - b);
  const byStatus = samples.reduce<Record<string, number>>((acc, s) => {
    acc[s.status] = (acc[s.status] ?? 0) + 1;
    return acc;
  }, {});
  const errors = samples.filter((s) => s.status === 0 || s.status >= 500).length;
  if (errors > 0) {
    failures.push(`${errors} requests failed with network errors or HTTP 5xx`);
  }
  const report = {
    runId,
    environment: {
      targets,
      bun: Bun.version,
      platform: `${process.platform}-${process.arch}`,
      concurrency,
      wallets: walletCount,
      hotShare,
    },
    methodology:
      'Closed-loop: N virtual providers send requests back-to-back for the duration. Mix: 63% BET, 18% WIN, 9% LOSS, 10% resend of a previous request (idempotent replay). ' +
      `${Math.round(hotShare * 100)}% of new operations target a single hot wallet. Latency measured client-side per request.`,
    requests: samples.length,
    durationS: Number(elapsedS.toFixed(2)),
    throughputRps: Number((samples.length / elapsedS).toFixed(1)),
    latencyMs: {
      p50: Number(percentile(latencies, 50).toFixed(1)),
      p95: Number(percentile(latencies, 95).toFixed(1)),
      p99: Number(percentile(latencies, 99).toFixed(1)),
      max: Number((latencies.at(-1) ?? 0).toFixed(1)),
    },
    statusCodes: byStatus,
    errorRate: Number((errors / Math.max(samples.length, 1)).toFixed(4)),
    replaysSent: samples.filter((s) => s.replay).length,
    serverCountersScope:
      metricsTargets.length === 1
        ? `sampled from ${metricsTargets[0]} (one replica per scrape if load-balanced)`
        : `summed over ${metricsTargets.length} instances`,
    serverCounters: {
      lockConflicts: (after.get('wagering_lock_conflicts_total') ?? 0) - (before.get('wagering_lock_conflicts_total') ?? 0),
      retries: sumMatching(after, 'wagering_retries_total') - sumMatching(before, 'wagering_retries_total'),
      duplicatesDetected: sumMatching(after, 'wagering_duplicates_total') - sumMatching(before, 'wagering_duplicates_total'),
    },
    outbox: {
      runEvents: runEventIds.length,
      unpublishedAfterDrain: pending,
      drainedAfterSeconds: drainedAfterS,
      publishLagSeconds: {
        p50: Number(percentile(lags, 50).toFixed(2)),
        p99: Number(percentile(lags, 99).toFixed(2)),
        max: Number((lags.at(-1) ?? 0).toFixed(2)),
      },
    },
    delivery: sink
      ? {
          expected: runEventIds.length,
          delivered: runEventIds.length - missing.length,
          missing: missing.length,
          duplicateDeliveries,
          otherEventsDrained: [...sink.seen.keys()].filter((id) => !runEventSet.has(id)).length,
        }
      : 'not verified (EVENTS_SINK=off)',
    reconciliation: { wallets: wallets.length, inconsistent: inconsistent.length },
    networkErrors: networkErrors.slice(0, 5),
    result: failures.length === 0 ? 'PASS' : 'FAIL',
    failures,
  };

  console.log(JSON.stringify(report, null, 2));
  const dir = join(import.meta.dir, 'results');
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${runId}.json`), JSON.stringify(report, null, 2));
  await db.end();
  if (failures.length > 0) {
    for (const failure of failures) console.error(`[load] FAIL: ${failure}`);
    process.exit(1);
  }
  console.log('[load] PASS: every event of the run was published and delivered; every wallet reconciles');
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
