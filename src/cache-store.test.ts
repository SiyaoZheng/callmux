import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DatabaseSync } from "node:sqlite";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { CallCache } from "./cache.js";
import { SqliteCacheStore } from "./cache-store.js";
import { errorResult } from "./results.js";

function textResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }] };
}

function persistentCache(
  path: string,
  ttlSeconds = 60,
  maxEntries = 100,
  maxEntryBytes = 1024 * 1024,
  maxTotalBytes = 16 * 1024 * 1024
): CallCache {
  return new CallCache(
    ttlSeconds,
    { allowTools: ["*"] },
    undefined,
    maxEntries,
    maxEntryBytes,
    maxTotalBytes,
    new SqliteCacheStore({ path, maxEntries, maxTotalBytes })
  );
}

test("SQLite cache restores the complete raw MCP result without a second load", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-persistent-cache-"));
  const path = join(dir, "cache.sqlite");
  const result: CallToolResult = {
    content: [
      { type: "text", text: "raw upstream body" },
      {
        type: "resource",
        resource: {
          uri: "https://example.test/result.json",
          mimeType: "application/json",
          text: "{\"answer\":42}",
        },
      },
    ],
    structuredContent: {
      answer: 42,
      nested: { preserved: true },
    },
    isError: false,
  };
  let downstreamCalls = 0;

  try {
    const first = persistentCache(path);
    const miss = await first.getOrLoad(
      {
        tool: "web_search_exa",
        server: "exa",
        scope: "headers:authorization=sha256:abc",
        args: { query: "persistent cache", numResults: 3 },
      },
      async () => {
        downstreamCalls++;
        return result;
      }
    );
    assert.equal(miss.source, "load");
    first.close();

    const second = persistentCache(path);
    const hit = await second.getOrLoad(
      {
        tool: "web_search_exa",
        server: "exa",
        scope: "headers:authorization=sha256:abc",
        args: { numResults: 3, query: "persistent cache" },
      },
      async () => {
        downstreamCalls++;
        return textResult("must not run");
      }
    );
    assert.equal(hit.source, "cache");
    assert.deepEqual(hit.result, result);
    assert.equal(downstreamCalls, 1);
    assert.deepEqual(second.stats().persistent, {
      enabled: true,
      path,
      entries: 1,
      storedBytes: Buffer.byteLength(JSON.stringify(result), "utf8"),
    });
    second.close();

    const db = new DatabaseSync(path);
    try {
      const row = db.prepare(`
        SELECT tool, server, scope, arguments_json, result_json
        FROM cache_entries
      `).get();
      assert.equal(row?.tool, "web_search_exa");
      assert.equal(row?.server, "exa");
      assert.equal(row?.scope, "headers:authorization=sha256:abc");
      assert.deepEqual(JSON.parse(String(row?.arguments_json)), {
        query: "persistent cache",
        numResults: 3,
      });
      assert.deepEqual(JSON.parse(String(row?.result_json)), result);
    } finally {
      db.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("SQLite cache preserves TTL and never persists tool errors", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-persistent-cache-ttl-"));
  const path = join(dir, "cache.sqlite");
  try {
    const first = persistentCache(path, 0.01);
    first.set("get_item", { id: 1 }, textResult("short-lived"));
    first.set("get_item", { id: 2 }, errorResult("downstream", "nope"));
    first.close();
    await new Promise((resolve) => setTimeout(resolve, 25));

    const second = persistentCache(path, 0.01);
    assert.equal(second.get("get_item", { id: 1 }), null);
    assert.equal(second.get("get_item", { id: 2 }), null);
    assert.equal(second.stats().persistent.entries, 0);
    second.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("SQLite cache enforces persistent LRU entry and byte ceilings", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-persistent-cache-lru-"));
  const path = join(dir, "cache.sqlite");
  const one = textResult("one");
  const two = textResult("two");
  const three = textResult("three");
  const maxTotalBytes =
    Buffer.byteLength(JSON.stringify(one), "utf8") +
    Buffer.byteLength(JSON.stringify(three), "utf8");
  try {
    const first = persistentCache(path, 60, 2, 1024, maxTotalBytes);
    first.set("get_item", { id: 1 }, one);
    first.set("get_item", { id: 2 }, two);
    assert.deepEqual(first.get("get_item", { id: 1 }), one);
    first.set("get_item", { id: 3 }, three);
    first.close();

    const second = persistentCache(path, 60, 2, 1024, maxTotalBytes);
    assert.deepEqual(second.get("get_item", { id: 1 }), one);
    assert.equal(second.get("get_item", { id: 2 }), null);
    assert.deepEqual(second.get("get_item", { id: 3 }), three);
    assert.equal(second.stats().persistent.entries, 2);
    assert.ok(second.stats().persistent.storedBytes <= maxTotalBytes);
    second.close();
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("SQLite cache migrates an earlier cache_entries table additively", async () => {
  const dir = await mkdtemp(join(tmpdir(), "callmux-persistent-cache-migration-"));
  const path = join(dir, "cache.sqlite");
  try {
    const legacy = new DatabaseSync(path);
    legacy.exec(`
      CREATE TABLE cache_entries (
        cache_key TEXT PRIMARY KEY,
        tool TEXT NOT NULL,
        result_json TEXT NOT NULL,
        expires_at_ms INTEGER NOT NULL,
        byte_size INTEGER NOT NULL
      )
    `);
    legacy.close();

    const store = new SqliteCacheStore({
      path,
      maxEntries: 100,
      maxTotalBytes: 1024 * 1024,
    });
    store.close();

    const migrated = new DatabaseSync(path);
    try {
      const columns = new Set(
        migrated.prepare("PRAGMA table_info(cache_entries)").all()
          .map((row) => String(row.name))
      );
      for (const column of [
        "server",
        "scope",
        "arguments_json",
        "created_at_ms",
        "accessed_at_ms",
        "read_only_hint",
        "idempotent_hint",
      ]) {
        assert.equal(columns.has(column), true, `expected migrated column ${column}`);
      }
      assert.equal(migrated.prepare("PRAGMA user_version").get()?.user_version, 1);
    } finally {
      migrated.close();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
