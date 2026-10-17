import { Controller, Get, Header, Inject, Res } from '@nestjs/common';
import type { Response } from 'express';
import { HEALTH_PROBES, type HealthProbes } from '../../application/ports';
import type { PrometheusMetrics } from '../../infrastructure/observability/prometheus-metrics';
import { Public } from './auth';

export const PROMETHEUS = Symbol('PrometheusMetrics');

const PROBE_TIMEOUT_MS = 2_000;

async function probe(fn: () => Promise<void>): Promise<{ status: 'up' | 'down'; latencyMs: number; error?: string }> {
  const started = performance.now();
  try {
    await Promise.race([
      fn(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error('probe timeout')), PROBE_TIMEOUT_MS).unref()),
    ]);
    return { status: 'up', latencyMs: Math.round(performance.now() - started) };
  } catch (error) {
    return {
      status: 'down',
      latencyMs: Math.round(performance.now() - started),
      error: describeProbeError(error),
    };
  }
}

function describeProbeError(error: unknown): string {
  if (!(error instanceof Error)) return 'unreachable';
  const code = (error as { code?: unknown }).code;
  // Connection errors can come with an empty message (e.g. AggregateError for ECONNREFUSED).
  return error.message || (typeof code === 'string' ? code : '') || error.name;
}

/** Unauthenticated by design. */
@Public()
@Controller('health')
export class HealthController {
  constructor(@Inject(HEALTH_PROBES) private readonly probes: HealthProbes) {}

  /** Liveness: the process and its event loop respond. No dependency checks (avoid restart storms). */
  @Get('live')
  live() {
    return { status: 'ok' };
  }

  /** Readiness: PostgreSQL and SQS reachable. 503 takes the instance out of rotation. */
  @Get('ready')
  async ready(@Res({ passthrough: true }) response: Response) {
    const [database, queue] = await Promise.all([probe(() => this.probes.database()), probe(() => this.probes.queue())]);
    const ok = database.status === 'up' && queue.status === 'up';
    response.status(ok ? 200 : 503);
    return { status: ok ? 'ok' : 'unavailable', checks: { database, queue } };
  }
}

@Public()
@Controller('metrics')
export class MetricsController {
  constructor(@Inject(PROMETHEUS) private readonly metrics: PrometheusMetrics) {}

  @Get()
  @Header('Content-Type', 'text/plain; version=0.0.4')
  async scrape(): Promise<string> {
    return this.metrics.registry.metrics();
  }
}
