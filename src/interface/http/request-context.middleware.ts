import type { NextFunction, Request, Response } from 'express';
import type { AppLogger } from '../../application/ports';
import { runWithLogContext } from '../../infrastructure/observability/logger';

const CORRELATION_HEADER = 'x-correlation-id';
const SAFE_ID = /^[\x21-\x7E]{1,128}$/;

/**
 * Establishes the async log context for the request (correlation id from the
 * `X-Correlation-Id` header or a new one) and writes one access-log line per
 * request: method, route, status, duration — never headers or bodies.
 */
export function requestContextMiddleware(logger: AppLogger, newId: () => string) {
  return (request: Request, response: Response, next: NextFunction): void => {
    const incoming = request.header(CORRELATION_HEADER);
    const correlationId = incoming && SAFE_ID.test(incoming) ? incoming : newId();
    response.setHeader('X-Correlation-Id', correlationId);
    const started = performance.now();

    runWithLogContext({ correlationId }, () => {
      response.on('finish', () => {
        if (request.path.startsWith('/health') || request.path === '/metrics') return;
        logger.info('http request', {
          method: request.method,
          path: request.route?.path ?? request.path,
          status: response.statusCode,
          durationMs: Math.round(performance.now() - started),
        });
      });
      next();
    });
  };
}
