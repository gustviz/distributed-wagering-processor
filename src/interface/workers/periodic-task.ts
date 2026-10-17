import type { AppLogger } from '../../application/ports';
import { runWithLogContext } from '../../infrastructure/observability/logger';

/**
 * Runs `tick` every `intervalMs`. When a tick reports a full batch it runs
 * again immediately (drains backlogs fast, idles cheaply). Errors are logged
 * and backed off; they never kill the loop. `stop()` waits for the running tick.
 */
export class PeriodicTask {
  private timer?: ReturnType<typeof setTimeout>;
  private running?: Promise<void>;
  private stopped = true;
  private consecutiveErrors = 0;

  constructor(
    private readonly name: string,
    private readonly intervalMs: number,
    private readonly tick: () => Promise<{ fullBatch: boolean }>,
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.schedule(0);
    this.logger.info('Worker started', { worker: this.name, intervalMs: this.intervalMs });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    await this.running;
    this.logger.info('Worker stopped', { worker: this.name });
  }

  private schedule(delayMs: number): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.running = this.runTick();
    }, delayMs);
  }

  private async runTick(): Promise<void> {
    let next = this.intervalMs;
    try {
      const { fullBatch } = await runWithLogContext({ correlationId: `${this.name}-tick` }, this.tick);
      this.consecutiveErrors = 0;
      if (fullBatch) next = 0;
    } catch (error) {
      this.consecutiveErrors++;
      next = Math.min(this.intervalMs * 2 ** Math.min(this.consecutiveErrors, 6), 30_000);
      this.logger.warn('Worker tick failed', { worker: this.name, failures: this.consecutiveErrors, err: String(error) });
    } finally {
      this.schedule(next);
    }
  }
}
