import Fastify, { type FastifyInstance } from 'fastify';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { clearSessionCookie, sessionToken, setSessionCookie } from './auth-http.js';

describe('HTTP session cookies', () => {
  let app: FastifyInstance;
  const token = 'test-session-with+encoded/characters=';

  beforeEach(async () => {
    app = Fastify();
    app.post('/api/login', (_request, reply) => {
      setSessionCookie(reply, token, new Date(Date.now() + 60_000));
      return { loggedIn: true };
    });
    app.get('/api/session', (request) => ({ token: sessionToken(request) }));
    app.post('/api/logout', (_request, reply) => {
      clearSessionCookie(reply);
      return reply.code(204).send();
    });
    app.get('/api/expired', (_request, reply) => {
      setSessionCookie(reply, token, new Date(0));
      return {};
    });
    await app.ready();
  });

  afterEach(async () => {
    await app.close();
  });

  it('allows an HTTP client to retain the session while preserving cookie restrictions', async () => {
    const login = await app.inject({ method: 'POST', url: '/api/login' });
    const cookie = login.cookies[0]!;
    expect(cookie.secure).not.toBe(true);
    expect(cookie.httpOnly).toBe(true);
    expect(cookie.sameSite).toBe('Strict');
    expect(cookie.path).toBe('/api');
    expect(cookie.maxAge).toBeGreaterThan(0);
    expect(cookie.maxAge).toBeLessThanOrEqual(60);
    // Return the cookie exactly as a browser would, without exposing it to page JavaScript.
    const session = await app.inject({
      method: 'GET',
      url: '/api/session',
      headers: { cookie: String(login.headers['set-cookie']).split(';')[0]! },
    });
    expect(session.json()).toEqual({ token });
  });

  it('clears the same cookie over HTTP on logout', async () => {
    const login = await app.inject({ method: 'POST', url: '/api/login' });
    const logout = await app.inject({ method: 'POST', url: '/api/logout' });
    const expired = logout.cookies[0]!;
    expect(expired.name).toBe(login.cookies[0]!.name);
    expect(expired.path).toBe(login.cookies[0]!.path);
    expect(expired.value).toBe('');
    expect(expired.maxAge).toBe(0);
    expect(expired.secure).not.toBe(true);
    expect(expired.httpOnly).toBe(true);
    expect(expired.sameSite).toBe('Strict');
  });

  it('does not extend an expired session cookie', async () => {
    const response = await app.inject({ method: 'GET', url: '/api/expired' });
    expect(response.cookies[0]!.maxAge).toBe(0);
  });
});
