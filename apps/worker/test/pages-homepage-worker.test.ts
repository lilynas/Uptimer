import { afterEach, describe, expect, it, vi } from 'vitest';

import pageWorker from '../../web/public/_worker.js';

type CacheMatcher = (request: Request) => Response | undefined;

function installDefaultCacheMock(match: CacheMatcher) {
  const put = vi.fn(async () => undefined);

  Object.defineProperty(globalThis, 'caches', {
    configurable: true,
    value: {
      default: {
        async match(request: Request) {
          return match(request)?.clone();
        },
        put,
      },
    },
  });

  return { put };
}

function setDeployId(value: string | undefined) {
  if (value === undefined) {
    delete (globalThis as { __UPTIMER_DEPLOY_ID__?: string }).__UPTIMER_DEPLOY_ID__;
    return;
  }

  Object.defineProperty(globalThis, '__UPTIMER_DEPLOY_ID__', {
    configurable: true,
    value,
  });
}

function makeEnv(indexHtml = '<!doctype html><html><head></head><body><div id="root"></div></body></html>') {
  return {
    ASSETS: {
      fetch: vi.fn(async () => new Response(indexHtml, { status: 200 })),
    },
    UPTIMER_API_ORIGIN: 'https://api.example.com',
  };
}

describe('pages homepage worker', () => {
  const originalCaches = (globalThis as { caches?: unknown }).caches;
  const originalFetch = globalThis.fetch;
  const originalDeployId = (globalThis as { __UPTIMER_DEPLOY_ID__?: string }).__UPTIMER_DEPLOY_ID__;

  afterEach(() => {
    if (originalCaches === undefined) {
      delete (globalThis as { caches?: unknown }).caches;
    } else {
      Object.defineProperty(globalThis, 'caches', {
        configurable: true,
        value: originalCaches,
      });
    }

    globalThis.fetch = originalFetch;
    setDeployId(originalDeployId);
    vi.restoreAllMocks();
  });

  it('serves cached injected HTML without calling the homepage API', async () => {
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/'
        ? new Response('<html>cached homepage</html>', { status: 200 })
        : undefined,
    );
    const env = makeEnv();
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('cached homepage');
    expect(env.ASSETS.fetch).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it('falls back to the cached injected homepage when snapshot fetch fails', async () => {
    setDeployId('deploy-a');
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: {
              'x-uptimer-generated-at': '1728000000',
              'x-uptimer-deploy-id': 'deploy-a',
            },
          })
        : undefined,
    );
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () => {
      throw new Error('network failed');
    }) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('fallback homepage');
  });

  it('serves a recent cached fallback homepage before calling the homepage API', async () => {
    setDeployId('deploy-a');
    const { put } = installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: {
              'Cache-Control': 'public, max-age=600',
              'x-uptimer-generated-at': '1728000000',
              'x-uptimer-deploy-id': 'deploy-a',
            },
          })
        : undefined,
    );
    const env = makeEnv();
    const fetchSpy = vi.fn();
    globalThis.fetch = fetchSpy as never;
    vi.spyOn(Date, 'now').mockReturnValue(1_728_000_030_000);

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    expect(await res.text()).toContain('fallback homepage');
    expect(res.headers.get('Cache-Control')).toBe(
      'public, max-age=30, stale-while-revalidate=0, stale-if-error=0',
    );
    expect(env.ASSETS.fetch).not.toHaveBeenCalled();
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(put).toHaveBeenCalledTimes(1);
  });

  it('ignores stale fallback html and fetches a fresh homepage artifact', async () => {
    setDeployId('deploy-a');
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: {
              'Cache-Control': 'public, max-age=600',
              'x-uptimer-generated-at': '1727999700',
              'x-uptimer-deploy-id': 'deploy-a',
            },
          })
        : undefined,
    );
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () =>
      new Response(
        JSON.stringify({
          generated_at: 1_728_000_000,
          preload_html: '<div id="uptimer-preload"><main>artifact preload</main></div>',
          snapshot: { site_title: 'Status Hub' },
          meta_title: 'Status Hub',
          meta_description: 'Production',
        }),
        { status: 200 },
      ),
    ) as never;
    vi.spyOn(Date, 'now').mockReturnValue(1_728_000_030_000);

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn((promise) => promise) },
    );

    expect(await res.text()).toContain('artifact preload');
    expect(env.ASSETS.fetch).toHaveBeenCalled();
    expect(globalThis.fetch).toHaveBeenCalled();
  });

  it('ignores reusable fallback html from a previous deploy and fetches a fresh homepage artifact', async () => {
    setDeployId('deploy-b');
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: {
              'Cache-Control': 'public, max-age=600',
              'x-uptimer-generated-at': '1728000000',
              'x-uptimer-deploy-id': 'deploy-a',
            },
          })
        : undefined,
    );
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () =>
      new Response(
        JSON.stringify({
          generated_at: 1_728_000_000,
          preload_html: '<div id="uptimer-preload"><main>artifact preload</main></div>',
          snapshot: { site_title: 'Status Hub' },
          meta_title: 'Status Hub',
          meta_description: 'Production',
        }),
        { status: 200 },
      ),
    ) as never;
    vi.spyOn(Date, 'now').mockReturnValue(1_728_000_030_000);

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn((promise) => promise) },
    );

    expect(await res.text()).toContain('artifact preload');
    expect(env.ASSETS.fetch).toHaveBeenCalled();
    expect(globalThis.fetch).toHaveBeenCalled();
  });

  it('returns the current base html when artifact fetch fails and fallback html is from a previous deploy', async () => {
    setDeployId('deploy-b');
    installDefaultCacheMock((request) =>
      request.url === 'https://status.example.com/__uptimer_homepage_fallback__'
        ? new Response('<html>fallback homepage</html>', {
            status: 200,
            headers: {
              'Cache-Control': 'public, max-age=600',
              'x-uptimer-generated-at': '1728000000',
              'x-uptimer-deploy-id': 'deploy-a',
            },
          })
        : undefined,
    );
    const env = makeEnv('<!doctype html><html><head></head><body><div id="root">base html</div></body></html>');
    globalThis.fetch = vi.fn(async () => {
      throw new Error('network failed');
    }) as never;
    vi.spyOn(Date, 'now').mockReturnValue(1_728_000_030_000);

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn() },
    );

    const html = await res.text();
    expect(html).toContain('base html');
    expect(html).not.toContain('fallback homepage');
  });

  it('injects the precomputed homepage artifact and updates both html caches on success', async () => {
    setDeployId('deploy-a');
    const { put } = installDefaultCacheMock(() => undefined);
    const env = makeEnv();
    globalThis.fetch = vi.fn(async () =>
      new Response(
        JSON.stringify({
          generated_at: 1_728_000_000,
          preload_html: '<div id="uptimer-preload"><main>artifact preload</main></div>',
          snapshot: { site_title: 'Status Hub' },
          meta_title: 'Status Hub',
          meta_description: 'Production',
        }),
        { status: 200 },
      ),
    ) as never;

    const res = await pageWorker.fetch(
      new Request('https://status.example.com/', {
        headers: { Accept: 'text/html' },
      }),
      env,
      { waitUntil: vi.fn((promise) => promise) },
    );

    const html = await res.text();
    expect(html).toContain('__UPTIMER_INITIAL_HOMEPAGE__');
    expect(html).not.toContain('__UPTIMER_INITIAL_STATUS__');
    expect(html).toContain('artifact preload');
    expect(html).not.toContain('__UPTIMER_BOOTSTRAP_FALLBACK__');
    expect(put).toHaveBeenCalledTimes(2);
  });
});
