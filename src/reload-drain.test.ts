import test from "node:test";
import assert from "node:assert/strict";
import { CallmuxListener } from "./listener.js";
import { CallCache } from "./cache.js";
import { UpstreamManager } from "./upstream.js";
import { resolveReloadDrainTimeoutMs } from "./reload-drain.js";

test("reload drain defaults to the longest configured call timeout plus grace", () => {
  assert.equal(
    resolveReloadDrainTimeoutMs({
      servers: {
        short: { command: "short", callTimeoutMs: 10_000 },
        long: { command: "long", callTimeoutMs: 240_000 },
      },
      callTimeoutMs: 120_000,
    }),
    241_000
  );
  assert.equal(
    resolveReloadDrainTimeoutMs({
      servers: {},
      reloadDrainTimeoutMs: 42_000,
    }),
    42_000
  );
});

test("listener drains the exact captured upstream generation", async () => {
  const oldUpstream = new UpstreamManager();
  const newUpstream = new UpstreamManager();
  const listener = new CallmuxListener({
    port: 0,
    config: { servers: {} },
    upstream: oldUpstream,
    cache: new CallCache(0),
    allTools: [],
    maxConcurrency: 10,
  });
  const internals = listener as unknown as {
    retainUpstream(upstream: UpstreamManager): () => void;
  };

  const releaseOld = internals.retainUpstream(oldUpstream);
  listener.applyReloadedState({
    config: { servers: {} },
    upstream: newUpstream,
    cache: new CallCache(0),
    allTools: [],
    maxConcurrency: 10,
  });

  assert.equal(await listener.waitForUpstreamDrain(newUpstream, 10), true);
  assert.equal(await listener.waitForUpstreamDrain(oldUpstream, 5), false);

  const drained = listener.waitForUpstreamDrain(oldUpstream, 1_000);
  releaseOld();
  assert.equal(await drained, true);
  await listener.close();
});
