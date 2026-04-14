import { connect } from 'cloudflare:sockets';

import { parseTcpTarget, validateTcpTarget } from './targets';
import type { CheckOutcome } from './types';

export type TcpCheckConfig = {
  target: string;
  timeoutMs: number;
};

type ParsedTcpTarget = { host: string; port: number };
type CachedTcpPreparation =
  | { parsed: ParsedTcpTarget; error: null }
  | { parsed: null; error: string };

const RETRY_DELAYS_MS = [300, 800] as const;
const cachedTcpPreparations = new Map<string, CachedTcpPreparation>();

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

function getCachedTcpPreparation(target: string): CachedTcpPreparation {
  const cached = cachedTcpPreparations.get(target);
  if (cached) {
    return cached;
  }

  const parsed = parseTcpTarget(target);
  if (!parsed) {
    const invalidPreparation: CachedTcpPreparation = {
      parsed: null,
      error: 'target must be in host:port format (IPv6: [addr]:port)',
    };
    cachedTcpPreparations.set(target, invalidPreparation);
    return invalidPreparation;
  }

  const targetErr = validateTcpTarget(target);
  if (targetErr) {
    const invalidPreparation: CachedTcpPreparation = {
      parsed: null,
      error: targetErr,
    };
    cachedTcpPreparations.set(target, invalidPreparation);
    return invalidPreparation;
  }

  const preparation: CachedTcpPreparation = {
    parsed,
    error: null,
  };
  cachedTcpPreparations.set(target, preparation);
  return preparation;
}

async function attemptTcpCheck(
  parsed: ParsedTcpTarget,
  timeoutMs: number,
): Promise<Omit<CheckOutcome, 'attempts'>> {
  const started = performance.now();
  let socket: ReturnType<typeof connect> | null = null;
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  try {
    socket = connect({ hostname: parsed.host, port: parsed.port });

    const opened = socket.opened.then(() => 'opened' as const).catch((err) => ({ err }));
    const timedOut = new Promise<'timeout'>((resolve) => {
      timeoutId = setTimeout(() => resolve('timeout'), timeoutMs);
    });

    const raced = await Promise.race([opened, timedOut]);
    const latencyMs = Math.round(performance.now() - started);

    if (raced === 'timeout') {
      socket.close();
      return {
        status: 'down',
        latencyMs,
        httpStatus: null,
        error: `Timeout after ${timeoutMs}ms`,
      };
    }

    if (typeof raced === 'object' && raced && 'err' in raced) {
      return {
        status: 'down',
        latencyMs,
        httpStatus: null,
        error: toErrorMessage((raced as { err: unknown }).err),
      };
    }

    socket.close();
    return { status: 'up', latencyMs, httpStatus: null, error: null };
  } catch (err) {
    const latencyMs = Math.round(performance.now() - started);
    return { status: 'down', latencyMs, httpStatus: null, error: toErrorMessage(err) };
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    try {
      socket?.close();
    } catch {
      // ignore
    }
  }
}

export async function runTcpCheck(config: TcpCheckConfig): Promise<CheckOutcome> {
  const preparation = getCachedTcpPreparation(config.target);
  if (preparation.error || !preparation.parsed) {
    return {
      status: 'unknown',
      latencyMs: null,
      httpStatus: null,
      error: preparation.error ?? 'Invalid target format',
      attempts: 1,
    };
  }

  const maxAttempts = 1 + RETRY_DELAYS_MS.length;
  let last: CheckOutcome | null = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const r = await attemptTcpCheck(preparation.parsed, config.timeoutMs);
    const outcome: CheckOutcome = { ...r, attempts: attempt };

    if (outcome.status === 'up') {
      return outcome;
    }
    if (outcome.status === 'unknown') {
      return outcome;
    }

    last = outcome;
    const delay = RETRY_DELAYS_MS[attempt - 1];
    if (delay !== undefined) {
      await sleep(delay);
    }
  }

  return (
    last ?? {
      status: 'unknown',
      latencyMs: null,
      httpStatus: null,
      error: 'No attempts executed',
      attempts: 0,
    }
  );
}
