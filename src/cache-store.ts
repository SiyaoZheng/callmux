import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { mkdirSync } from "node:fs";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { DatabaseSync, StatementSync } from "node:sqlite";

export interface PersistentCacheRecord {
  key: string;
  tool: string;
  args?: Record<string, unknown>;
  server?: string;
  scope?: string;
  result: CallToolResult;
  expiresAt: number;
  byteSize: number;
  createdAt: number;
  accessedAt: number;
  readOnlyHint?: boolean;
  idempotentHint?: boolean;
}

export interface PersistentCacheStats {
  entries: number;
  storedBytes: number;
}

export interface PersistentCacheStore {
  readonly path: string;
  get(key: string, now?: number): PersistentCacheRecord | null;
  touch(key: string, now?: number): void;
  set(record: PersistentCacheRecord, now?: number): void;
  delete(key: string): void;
  invalidate(tool?: string, server?: string): void;
  prune(now?: number): void;
  stats(now?: number): PersistentCacheStats;
  close(): void;
}

interface SqliteCacheStoreOptions {
  path: string;
  maxEntries: number;
  maxTotalBytes: number;
}

type DatabaseSyncConstructor = new (path: string) => DatabaseSync;

const SCHEMA_SQL = `
PRAGMA busy_timeout = 5000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS cache_entries (
  cache_key TEXT PRIMARY KEY,
  tool TEXT NOT NULL,
  server TEXT,
  scope TEXT,
  arguments_json TEXT,
  result_json TEXT NOT NULL,
  expires_at_ms INTEGER NOT NULL,
  byte_size INTEGER NOT NULL,
  created_at_ms INTEGER NOT NULL,
  accessed_at_ms INTEGER NOT NULL,
  read_only_hint INTEGER NOT NULL DEFAULT 0,
  idempotent_hint INTEGER NOT NULL DEFAULT 0
);
`;

const INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_cache_entries_expiry
  ON cache_entries(expires_at_ms);
CREATE INDEX IF NOT EXISTS idx_cache_entries_access
  ON cache_entries(accessed_at_ms, created_at_ms);
CREATE INDEX IF NOT EXISTS idx_cache_entries_tool_server
  ON cache_entries(tool, server);
`;

function loadDatabaseSync(): DatabaseSyncConstructor {
  try {
    const require = createRequire(import.meta.url);
    const sqlite = require("node:sqlite") as { DatabaseSync?: DatabaseSyncConstructor };
    if (typeof sqlite.DatabaseSync === "function") return sqlite.DatabaseSync;
  } catch (error) {
    throw new Error(
      `persistentCache requires Node 24 or newer with node:sqlite: ${(error as Error).message}`
    );
  }
  throw new Error("persistentCache requires Node 24 or newer with node:sqlite");
}

function optionalText(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function requiredNumber(value: unknown, column: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`invalid persistent cache ${column}`);
  }
  return value;
}

function parseArguments(value: unknown): Record<string, unknown> | undefined {
  if (typeof value !== "string") return undefined;
  const parsed = JSON.parse(value) as unknown;
  return parsed !== null && typeof parsed === "object" && !Array.isArray(parsed)
    ? parsed as Record<string, unknown>
    : undefined;
}

function parseResult(value: unknown): CallToolResult {
  if (typeof value !== "string") throw new Error("invalid persistent cache result_json");
  const parsed = JSON.parse(value) as Partial<CallToolResult> | null;
  if (!parsed || !Array.isArray(parsed.content)) {
    throw new Error("invalid persistent cache result payload");
  }
  return parsed as CallToolResult;
}

export class SqliteCacheStore implements PersistentCacheStore {
  readonly path: string;
  private readonly db: DatabaseSync;
  private readonly maxEntries: number;
  private readonly maxTotalBytes: number;
  private readonly getStmt: StatementSync;
  private readonly touchStmt: StatementSync;
  private readonly upsertStmt: StatementSync;
  private readonly deleteStmt: StatementSync;
  private readonly deleteExpiredStmt: StatementSync;
  private readonly deleteAllStmt: StatementSync;
  private readonly deleteServerStmt: StatementSync;
  private readonly deleteToolStmt: StatementSync;
  private readonly deleteToolServerStmt: StatementSync;
  private readonly orderedRowsStmt: StatementSync;
  private readonly statsStmt: StatementSync;
  private closed = false;

  constructor(options: SqliteCacheStoreOptions, Database = loadDatabaseSync()) {
    this.path = options.path;
    this.maxEntries = options.maxEntries;
    this.maxTotalBytes = options.maxTotalBytes;
    mkdirSync(dirname(this.path), { recursive: true });
    this.db = new Database(this.path);
    this.db.exec(SCHEMA_SQL);
    this.migrateColumns();
    this.db.exec(INDEX_SQL);
    this.db.exec("PRAGMA user_version = 1");

    this.getStmt = this.db.prepare(`
      SELECT cache_key, tool, server, scope, arguments_json, result_json,
        expires_at_ms, byte_size, created_at_ms, accessed_at_ms,
        read_only_hint, idempotent_hint
      FROM cache_entries
      WHERE cache_key = ?
    `);
    this.touchStmt = this.db.prepare(
      "UPDATE cache_entries SET accessed_at_ms = ? WHERE cache_key = ?"
    );
    this.upsertStmt = this.db.prepare(`
      INSERT INTO cache_entries (
        cache_key, tool, server, scope, arguments_json, result_json,
        expires_at_ms, byte_size, created_at_ms, accessed_at_ms,
        read_only_hint, idempotent_hint
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(cache_key) DO UPDATE SET
        tool = excluded.tool,
        server = excluded.server,
        scope = excluded.scope,
        arguments_json = excluded.arguments_json,
        result_json = excluded.result_json,
        expires_at_ms = excluded.expires_at_ms,
        byte_size = excluded.byte_size,
        created_at_ms = excluded.created_at_ms,
        accessed_at_ms = excluded.accessed_at_ms,
        read_only_hint = excluded.read_only_hint,
        idempotent_hint = excluded.idempotent_hint
    `);
    this.deleteStmt = this.db.prepare("DELETE FROM cache_entries WHERE cache_key = ?");
    this.deleteExpiredStmt = this.db.prepare(
      "DELETE FROM cache_entries WHERE expires_at_ms < ?"
    );
    this.deleteAllStmt = this.db.prepare("DELETE FROM cache_entries");
    this.deleteServerStmt = this.db.prepare("DELETE FROM cache_entries WHERE server = ?");
    this.deleteToolStmt = this.db.prepare("DELETE FROM cache_entries WHERE tool = ?");
    this.deleteToolServerStmt = this.db.prepare(
      "DELETE FROM cache_entries WHERE tool = ? AND server = ?"
    );
    this.orderedRowsStmt = this.db.prepare(`
      SELECT cache_key, byte_size
      FROM cache_entries
      ORDER BY accessed_at_ms DESC, created_at_ms DESC, cache_key DESC
    `);
    this.statsStmt = this.db.prepare(`
      SELECT COUNT(*) AS entries, COALESCE(SUM(byte_size), 0) AS stored_bytes
      FROM cache_entries
    `);
    this.prune();
  }

  /** Additive migration for databases created by earlier persistent-cache builds. */
  private migrateColumns(): void {
    const columns = this.db.prepare("PRAGMA table_info(cache_entries)").all();
    const names = new Set(columns.map((column) => String(column.name)));
    const additions: Array<[string, string]> = [
      ["server", "TEXT"],
      ["scope", "TEXT"],
      ["arguments_json", "TEXT"],
      ["created_at_ms", "INTEGER NOT NULL DEFAULT 0"],
      ["accessed_at_ms", "INTEGER NOT NULL DEFAULT 0"],
      ["read_only_hint", "INTEGER NOT NULL DEFAULT 0"],
      ["idempotent_hint", "INTEGER NOT NULL DEFAULT 0"],
    ];
    for (const [name, definition] of additions) {
      if (!names.has(name)) {
        this.db.exec(`ALTER TABLE cache_entries ADD COLUMN ${name} ${definition}`);
      }
    }
  }

  get(key: string, now = Date.now()): PersistentCacheRecord | null {
    const row = this.getStmt.get(key);
    if (!row) return null;
    const expiresAt = requiredNumber(row.expires_at_ms, "expires_at_ms");
    if (now > expiresAt) {
      this.deleteStmt.run(key);
      return null;
    }

    try {
      const result = parseResult(row.result_json);
      if (result.isError) {
        this.deleteStmt.run(key);
        return null;
      }
      this.touchStmt.run(now, key);
      return {
        key: String(row.cache_key),
        tool: String(row.tool),
        ...(optionalText(row.server) ? { server: optionalText(row.server) } : {}),
        ...(optionalText(row.scope) ? { scope: optionalText(row.scope) } : {}),
        ...(row.arguments_json !== null
          ? { args: parseArguments(row.arguments_json) }
          : {}),
        result,
        expiresAt,
        byteSize: requiredNumber(row.byte_size, "byte_size"),
        createdAt: requiredNumber(row.created_at_ms, "created_at_ms"),
        accessedAt: now,
        ...(row.read_only_hint === 1 ? { readOnlyHint: true } : {}),
        ...(row.idempotent_hint === 1 ? { idempotentHint: true } : {}),
      };
    } catch {
      this.deleteStmt.run(key);
      return null;
    }
  }

  touch(key: string, now = Date.now()): void {
    this.touchStmt.run(now, key);
  }

  set(record: PersistentCacheRecord, now = Date.now()): void {
    if (record.result.isError) return;
    const resultJson = JSON.stringify(record.result);
    if (resultJson === undefined) return;
    const argumentsJson = record.args === undefined
      ? null
      : JSON.stringify(record.args);

    this.transaction(() => {
      this.upsertStmt.run(
        record.key,
        record.tool,
        record.server ?? null,
        record.scope ?? null,
        argumentsJson ?? null,
        resultJson,
        record.expiresAt,
        record.byteSize,
        record.createdAt,
        record.accessedAt,
        record.readOnlyHint === true ? 1 : 0,
        record.idempotentHint === true ? 1 : 0
      );
      this.pruneInternal(now);
    });
  }

  delete(key: string): void {
    this.deleteStmt.run(key);
  }

  invalidate(tool?: string, server?: string): void {
    if (tool !== undefined && server !== undefined) {
      this.deleteToolServerStmt.run(tool, server);
    } else if (tool !== undefined) {
      this.deleteToolStmt.run(tool);
    } else if (server !== undefined) {
      this.deleteServerStmt.run(server);
    } else {
      this.deleteAllStmt.run();
    }
  }

  prune(now = Date.now()): void {
    this.transaction(() => this.pruneInternal(now));
  }

  private pruneInternal(now: number): void {
    this.deleteExpiredStmt.run(now);
    const rows = this.orderedRowsStmt.all();
    let retainedEntries = 0;
    let retainedBytes = 0;
    for (const row of rows) {
      const key = String(row.cache_key);
      const byteSize = requiredNumber(row.byte_size, "byte_size");
      const retain =
        retainedEntries < this.maxEntries &&
        retainedBytes + byteSize <= this.maxTotalBytes;
      if (retain) {
        retainedEntries++;
        retainedBytes += byteSize;
      } else {
        this.deleteStmt.run(key);
      }
    }
  }

  stats(now = Date.now()): PersistentCacheStats {
    this.prune(now);
    const row = this.statsStmt.get();
    return {
      entries: requiredNumber(row?.entries ?? 0, "entries"),
      storedBytes: requiredNumber(row?.stored_bytes ?? 0, "stored_bytes"),
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.db.close();
  }

  private transaction(action: () => void): void {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      action();
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
}

export function openSqliteCacheStore(options: SqliteCacheStoreOptions): PersistentCacheStore {
  return new SqliteCacheStore(options);
}

export function defaultPersistentCachePath(): string {
  return join(homedir(), ".config", "callmux", "callmux-cache.sqlite");
}
