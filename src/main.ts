import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import express, { type NextFunction, type Request, type Response } from 'express';
import { AppModule } from './app.module';
import { type AppConfig, loadConfig } from './config/app-config';
import { JsonLogger } from './infrastructure/observability/logger';
import { UuidV7Generator } from './infrastructure/support';
import { requestContextMiddleware } from './interface/http/request-context.middleware';

export async function createApp(config: AppConfig): Promise<NestExpressApplication> {
  const logger = new JsonLogger({ service: config.serviceName, instanceId: config.instanceId }, config.logLevel);
  const app = await NestFactory.create<NestExpressApplication>(AppModule.forRoot(config, logger), {
    logger,
    bodyParser: false,
    abortOnError: false,
  });
  const ids = new UuidV7Generator();
  app.use(requestContextMiddleware(logger, () => ids.next()));
  app.use(express.json({ limit: '64kb' }));
  // Malformed JSON must be a 400 with the standard error body, not Express' HTML page.
  app.use((error: unknown, _req: Request, res: Response, next: NextFunction) => {
    if (error && typeof error === 'object' && 'type' in error && (error as { type: string }).type.startsWith('entity.')) {
      res.status(400).json({ error: { code: 'INVALID_PAYLOAD', message: 'Malformed JSON body', retryable: false } });
      return;
    }
    next(error);
  });
  app.disable('x-powered-by');
  app.enableShutdownHooks(['SIGTERM', 'SIGINT']);
  return app;
}

async function bootstrap(): Promise<void> {
  const config = loadConfig();
  const app = await createApp(config);
  if (config.http.enabled) {
    await app.listen(config.http.port, '0.0.0.0');
  } else {
    await app.init();
  }
}

if (import.meta.main) {
  bootstrap().catch((error: unknown) => {
    process.stderr.write(
      `${JSON.stringify({ level: 'error', msg: 'bootstrap failed', error: error instanceof Error ? error.message : String(error) })}\n`,
    );
    process.exit(1);
  });
}
