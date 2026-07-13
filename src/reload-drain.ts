import type { CallmuxListener } from "./listener.js";
import type { CallmuxProxy } from "./proxy.js";
import type { CallmuxConfig } from "./types.js";

const DEFAULT_CALL_TIMEOUT_MS = 180_000;
const RELOAD_DRAIN_GRACE_MS = 1_000;

/**
 * By default, allow every configured downstream timeout to elapse, plus a
 * small amount of bookkeeping grace. Operators can set an explicit hard
 * deadline when a shorter or longer reload window is preferable.
 */
export function resolveReloadDrainTimeoutMs(config: CallmuxConfig): number {
  if (config.reloadDrainTimeoutMs !== undefined) {
    return config.reloadDrainTimeoutMs;
  }

  let longestCallTimeoutMs = config.callTimeoutMs ?? DEFAULT_CALL_TIMEOUT_MS;
  for (const server of Object.values(config.servers)) {
    if (server.callTimeoutMs !== undefined) {
      longestCallTimeoutMs = Math.max(longestCallTimeoutMs, server.callTimeoutMs);
    }
  }
  return longestCallTimeoutMs + RELOAD_DRAIN_GRACE_MS;
}

export async function drainAndCloseProxy(
  listener: CallmuxListener,
  proxy: CallmuxProxy,
  config: CallmuxConfig,
  reportError: (message: string) => void,
  signal?: AbortSignal
): Promise<void> {
  const timeoutMs = resolveReloadDrainTimeoutMs(config);
  const drained = await listener.waitForUpstreamDrain(
    proxy.getUpstream(),
    timeoutMs,
    signal
  );
  if (!drained && !signal?.aborted) {
    reportError(
      `Stale upstream generation still had active calls after ${timeoutMs}ms; forcing close`
    );
  }

  try {
    await proxy.close();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    reportError(`Stale upstream close failed: ${message}`);
  }
}
