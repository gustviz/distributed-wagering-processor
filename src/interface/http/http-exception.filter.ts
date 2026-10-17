import { type ArgumentsHost, Catch, type ExceptionFilter, HttpException, Inject } from '@nestjs/common';
import type { Response } from 'express';
import { APP_LOGGER, type AppLogger } from '../../application/ports';
import { classifyError, type ErrorCategory } from '../error-classification';

const STATUS: Record<ErrorCategory, number> = {
  invalid: 400,
  not_found: 404,
  conflict: 409,
  transient: 503,
  internal: 500,
};

/**
 * Uniform error body for every endpoint:
 *   { "error": { "code": "...", "message": "...", "retryable": bool, "details"?: [...] } }
 * `retryable` + status let a provider decide without parsing messages.
 * Business rejections are not errors and never reach this filter (see controller).
 */
@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  constructor(@Inject(APP_LOGGER) private readonly logger: AppLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      response.status(status).json({
        error: {
          code: status === 404 ? 'NOT_FOUND' : status === 401 ? 'UNAUTHORIZED' : 'HTTP_ERROR',
          message: exception.message,
          retryable: status >= 500,
        },
      });
      return;
    }
    if (exception instanceof SyntaxError) {
      response.status(400).json({ error: { code: 'INVALID_PAYLOAD', message: 'Malformed JSON body', retryable: false } });
      return;
    }

    const classified = classifyError(exception);
    const status = STATUS[classified.category];
    if (classified.category === 'internal') {
      this.logger.error('Unhandled error', { code: classified.code }, exception);
    } else if (classified.category === 'transient') {
      this.logger.warn('Transient failure', { code: classified.code, err: String(exception) });
    }
    if (classified.category === 'transient') response.setHeader('Retry-After', '1');
    response.status(status).json({
      error: {
        code: classified.code,
        message: classified.message,
        retryable: classified.retryable,
        ...(classified.details ? { details: classified.details } : {}),
      },
    });
  }
}
