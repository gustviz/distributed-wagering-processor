import {
  type CanActivate,
  type ExecutionContext,
  Inject,
  Injectable,
  SetMetadata,
  UnauthorizedException,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import type { Request } from 'express';

/**
 * Authentication extension point (see ARCHITECTURE.md §Authentication).
 *
 * Authentication is intentionally NOT implemented (it carries no points and
 * must not compete with correctness). The seam is explicit:
 *  - `ProviderIdentityPort` resolves the caller from the request; the no-op
 *    adapter returns an anonymous principal.
 *  - `AuthGuard` is registered globally; `@Public()` opts endpoints out
 *    (health checks and metrics stay open even once real auth is plugged in).
 *
 * Intended design: an external IdP (Keycloak/Zitadel) issues client-credentials
 * JWTs per provider; an `OidcProviderIdentity` adapter validates them via JWKS
 * and returns `{ providerId }` from a claim; the controller then requires
 * `principal.providerId === body.providerId`. Queue messages are an internal
 * trusted channel, but their providerId still goes through the same domain rules.
 */
export interface ProviderPrincipal {
  type: 'anonymous' | 'provider';
  providerId?: string;
}

export interface ProviderIdentityPort {
  authenticate(request: Request): Promise<ProviderPrincipal | undefined>;
}

export const PROVIDER_IDENTITY = Symbol('ProviderIdentityPort');
const IS_PUBLIC = 'auth:isPublic';

export const Public = () => SetMetadata(IS_PUBLIC, true);

/** No-op adapter: every caller is accepted as anonymous. Replace with an OIDC adapter. */
export class NoopProviderIdentity implements ProviderIdentityPort {
  async authenticate(): Promise<ProviderPrincipal> {
    return { type: 'anonymous' };
  }
}

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    @Inject(PROVIDER_IDENTITY) private readonly identity: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    const request = context.switchToHttp().getRequest<Request & { principal?: ProviderPrincipal }>();
    const principal = await this.identity.authenticate(request);
    if (!principal) throw new UnauthorizedException();
    request.principal = principal;
    return true;
  }
}
