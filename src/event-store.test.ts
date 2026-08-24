import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { openEventStore } from "./event-store.js";

const T0 = Date.UTC(2026, 5, 23, 12, 0, 0);

test("event store records call rows and drill-down breakdowns", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-event-store-"));
  const path = join(dir, "events.sqlite");
  const store = await openEventStore({
    path,
    now: () => T0,
  });
  try {
    store.recordCall({
      timestampMs: T0 - 1_000,
      server: "github",
      tool: "github__issue_read",
      arguments: { query: "中文 test", nested: { limit: 5 }, flags: [true, false] },
      targetTool: "issue_read",
      sessionId: "session-a",
      principal: "bearer:ops",
      transport: "cli",
      durationMs: 25,
      ok: true,
      status: "ok",
      bytesIn: 100,
      bytesOut: 200,
      toolKind: "downstream",
      operation: "direct",
      downstreamCalls: 1,
      targets: [{ server: "github", tool: "issue_read", count: 1 }],
      forwardedHeaders: ["Authorization"],
    });
    store.recordCall({
      timestampMs: T0,
      server: "github",
      tool: "github__issue_write",
      targetTool: "issue_write",
      sessionId: "session-a",
      principal: "bearer:ops",
      transport: "mcp",
      durationMs: 75,
      ok: false,
      status: "error",
      errorClass: "tool_call_failed",
      bytesIn: 50,
      bytesOut: 150,
      toolKind: "downstream",
      operation: "direct",
      downstreamCalls: 1,
      targets: [{ server: "github", tool: "issue_write", count: 1 }],
    });

    const drilldown = await store.queryDrilldown({ fromMs: T0 - 10_000, toMs: T0 + 10_000 });
    assert.equal(drilldown.totals.calls, 2);
    assert.equal(drilldown.totals.errors, 1);
    assert.equal(drilldown.totals.avgDurationMs, 50);
    assert.equal(drilldown.totals.bytesIn, 150);
    assert.equal(drilldown.totals.bytesOut, 350);
    assert.equal(drilldown.byServer[0].name, "github");
    assert.equal(drilldown.byServer[0].calls, 2);
    assert.equal(drilldown.byTool.some((row) => row.name === "issue_read"), true);
    assert.equal(drilldown.bySession[0].name, "session-a");
    assert.equal(drilldown.byTransport.find((row) => row.name === "cli")?.calls, 1);
    assert.equal(drilldown.byTransport.find((row) => row.name === "mcp")?.calls, 1);
    assert.deepEqual(drilldown.forwardedHeaders, [{
      server: "github",
      tool: "issue_read",
      sessionId: "session-a",
      principal: "bearer:ops",
      headerName: "authorization",
      calls: 1,
      lastSeenAt: new Date(T0 - 1_000).toISOString(),
    }]);
    await store.flush();
    const sqlite = await import("node:sqlite");
    const db = new sqlite.DatabaseSync(path);
    try {
      const rows = db
        .prepare("SELECT tool, arguments_json FROM call_events ORDER BY id")
        .all()
        .map((row) => ({ tool: row.tool, arguments_json: row.arguments_json }));
      assert.deepEqual(rows, [
        {
          tool: "github__issue_read",
          arguments_json: JSON.stringify({
            query: "中文 test",
            nested: { limit: 5 },
            flags: [true, false],
          }),
        },
        { tool: "github__issue_write", arguments_json: null },
      ]);
    } finally {
      db.close();
    }
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("event store prunes by max rows and age", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-event-prune-"));
  const store = await openEventStore({
    path: join(dir, "events.sqlite"),
    maxRows: 2,
    retentionDays: 1,
    pruneEvery: 1,
    now: () => T0,
  });
  try {
    store.recordCall({
      timestampMs: T0 - 2 * 24 * 60 * 60_000,
      tool: "old",
      durationMs: 1,
      ok: true,
    });
    store.recordCall({ timestampMs: T0 - 2_000, tool: "one", durationMs: 1, ok: true });
    store.recordCall({ timestampMs: T0 - 1_000, tool: "two", durationMs: 1, ok: true });
    store.recordCall({ timestampMs: T0, tool: "three", durationMs: 1, ok: true });

    const drilldown = await store.queryDrilldown({ fromMs: T0 - 3 * 24 * 60 * 60_000, toMs: T0 + 1 });
    assert.equal(drilldown.totals.calls, 2);
    assert.equal(drilldown.byTool.some((row) => row.name === "old"), false);
    assert.equal(drilldown.byTool.some((row) => row.name === "one"), false);
    assert.deepEqual(drilldown.byTool.map((row) => row.name).sort(), ["three", "two"]);
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("event store migrates an existing database that predates optional call columns", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-event-migrate-"));
  const path = join(dir, "events.sqlite");

  const sqlite = await import("node:sqlite");
  const legacyDb = new sqlite.DatabaseSync(path);
  legacyDb.exec(`
    CREATE TABLE call_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_ms INTEGER NOT NULL,
      ts TEXT NOT NULL,
      server TEXT,
      tool TEXT NOT NULL,
      target_tool TEXT,
      session_id TEXT,
      principal TEXT,
      duration_ms INTEGER NOT NULL,
      ok INTEGER NOT NULL,
      status TEXT,
      error_class TEXT,
      bytes_in INTEGER NOT NULL DEFAULT 0,
      bytes_out INTEGER NOT NULL DEFAULT 0,
      cache_hit INTEGER NOT NULL DEFAULT 0,
      tool_kind TEXT,
      operation TEXT,
      downstream_calls INTEGER NOT NULL DEFAULT 0
    );
  `);
  legacyDb
    .prepare(`
      INSERT INTO call_events (
        ts_ms, ts, server, tool, target_tool, session_id, principal, duration_ms,
        ok, status, error_class, bytes_in, bytes_out, cache_hit, tool_kind,
        operation, downstream_calls
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `)
    .run(
      T0 - 5_000,
      new Date(T0 - 5_000).toISOString(),
      "github",
      "github__issue_read",
      "issue_read",
      "session-legacy",
      "bearer:ops",
      10,
      1,
      "downstream_error",
      null,
      10,
      20,
      0,
      "downstream",
      "direct",
      1
    );
  legacyDb.close();

  const store = await openEventStore({ path, now: () => T0 });
  try {
    store.recordCall({
      timestampMs: T0,
      tool: "new_tool",
      arguments: { query: "after migration" },
      transport: "cli",
      durationMs: 5,
      ok: true,
    });

    const drilldown = await store.queryDrilldown({ fromMs: T0 - 60_000, toMs: T0 + 1 });
    assert.equal(drilldown.totals.calls, 2);
    assert.equal(drilldown.totals.errors, 1);
    // Pre-migration rows have no transport recorded; they group under "mcp".
    assert.equal(drilldown.byTransport.find((row) => row.name === "mcp")?.calls, 1);
    assert.equal(drilldown.byTransport.find((row) => row.name === "cli")?.calls, 1);
    await store.flush();
    const migratedDb = new sqlite.DatabaseSync(path);
    try {
      const row = migratedDb
        .prepare("SELECT arguments_json, ok FROM call_events WHERE tool = ?")
        .get("new_tool");
      assert.equal(row?.arguments_json, JSON.stringify({ query: "after migration" }));
      const legacyRow = migratedDb
        .prepare("SELECT ok FROM call_events WHERE tool = ?")
        .get("github__issue_read");
      assert.equal(legacyRow?.ok, 0);
    } finally {
      migratedDb.close();
    }
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("event store batches queued writes before sending them to the SQLite worker", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-event-batch-"));
  const store = await openEventStore({ path: join(dir, "events.sqlite") });
  const worker = (store as unknown as {
    worker: { postMessage: (message: unknown) => void };
  }).worker;
  const originalPostMessage = worker.postMessage.bind(worker);
  const batchSizes: number[] = [];
  worker.postMessage = (message: unknown) => {
    const command = message as { type?: string; samples?: unknown[] };
    if (command.type === "record") batchSizes.push(command.samples?.length ?? 0);
    originalPostMessage(message);
  };

  try {
    for (let index = 0; index < 205; index++) {
      store.recordCall({ tool: `tool-${index}`, durationMs: 1, ok: true });
    }
    await store.flush();
    assert.deepEqual(batchSizes, [100, 100, 5]);
    assert.equal((await store.queryDrilldown()).totals.calls, 205);
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("event store cleans up a synchronous worker post failure", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-event-clone-failure-"));
  const reported: Error[] = [];
  const store = await openEventStore({
    path: join(dir, "events.sqlite"),
    onError: (error) => {
      reported.push(error);
      throw new Error("observer failure must be isolated");
    },
  });

  try {
    store.recordCall({
      tool: "invalid-sample",
      durationMs: 1,
      ok: true,
      nonCloneable: () => undefined,
    } as never);
    await assert.rejects(store.flush(), /clone|function/i);
    assert.equal(
      (store as unknown as { pendingRequests: Map<number, unknown> }).pendingRequests.size,
      0
    );
    assert.equal(reported.length, 1);
  } finally {
    await store.close();
    await rm(dir, { recursive: true, force: true });
  }
});
