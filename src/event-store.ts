import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} from "node:worker_threads";
import {
  buildResearchIndex,
  extractResearchObservations,
  type ResearchIndex,
  type ResearchObservations,
  type ResearchProvider,
} from "./research-index.js";

export const DEFAULT_EVENT_STORE_MAX_ROWS = 100_000;
export const DEFAULT_EVENT_STORE_RETENTION_DAYS = 14;
export const DEFAULT_EVENT_STORE_PRUNE_EVERY = 100;

interface EventStoreOptions {
  path: string;
  maxRows?: number;
  retentionDays?: number;
  pruneEvery?: number;
  now?: () => number;
  /** Receives asynchronous worker/queue failures without blocking call completion. */
  onError?: (error: Error) => void;
}

interface StatementSync {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: unknown[]): Record<string, unknown> | undefined;
  all(...params: unknown[]): Record<string, unknown>[];
}

interface DatabaseSync {
  exec(sql: string): void;
  prepare(sql: string): StatementSync;
  close(): void;
}

interface DatabaseSyncConstructor {
  new(path: string): DatabaseSync;
}

interface EventTargetSample {
  server?: string;
  tool: string;
  count?: number;
}

export interface EventStoreCallSample {
  timestampMs?: number;
  server?: string;
  tool: string;
  arguments?: unknown;
  targetTool?: string;
  sessionId?: string;
  agentSignature?: string;
  projectName?: string;
  projectPath?: string;
  principal?: string;
  /** How the calling client reached the listener: the `callmux` CLI verbs vs any MCP client */
  transport?: "cli" | "mcp";
  durationMs: number;
  ok: boolean;
  status?: string;
  errorClass?: string;
  bytesIn?: number;
  bytesOut?: number;
  cacheHit?: boolean;
  toolKind?: "callmux_meta" | "downstream";
  operation?: string;
  downstreamCalls?: number;
  targets?: EventTargetSample[];
  forwardedHeaders?: string[];
  research?: ResearchObservations;
}

export type EventStoreResearchIndex = ResearchIndex;

interface EventStoreBreakdownRow {
  name: string;
  calls: number;
  errors: number;
  avgDurationMs: number;
  bytesIn: number;
  bytesOut: number;
  lastCallAt: string;
}

interface EventStoreForwardedHeaderRow {
  server: string;
  tool: string;
  sessionId: string;
  principal: string;
  headerName: string;
  calls: number;
  lastSeenAt: string;
}

export interface EventStoreDrilldown {
  totals: {
    calls: number;
    errors: number;
    avgDurationMs: number;
    bytesIn: number;
    bytesOut: number;
  };
  byServer: EventStoreBreakdownRow[];
  byTool: EventStoreBreakdownRow[];
  bySession: EventStoreBreakdownRow[];
  byTransport: EventStoreBreakdownRow[];
  forwardedHeaders: EventStoreForwardedHeaderRow[];
}

const SCHEMA_SQL = `
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS call_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  ts TEXT NOT NULL,
  server TEXT,
  tool TEXT NOT NULL,
  arguments_json TEXT,
  target_tool TEXT,
  session_id TEXT,
  agent_signature TEXT,
  project_name TEXT,
  project_path TEXT,
  principal TEXT,
  transport TEXT,
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

CREATE TABLE IF NOT EXISTS call_event_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL REFERENCES call_events(id) ON DELETE CASCADE,
  server TEXT,
  tool TEXT NOT NULL,
  count INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS forwarded_header_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL REFERENCES call_events(id) ON DELETE CASCADE,
  ts_ms INTEGER NOT NULL,
  ts TEXT NOT NULL,
  server TEXT NOT NULL,
  tool TEXT NOT NULL,
  session_id TEXT,
  principal TEXT,
  header_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_queries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL REFERENCES call_events(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  query TEXT NOT NULL,
  UNIQUE(event_id, provider, query)
);

CREATE TABLE IF NOT EXISTS web_page_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL REFERENCES call_events(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  url TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  domain TEXT NOT NULL,
  path TEXT NOT NULL,
  relation TEXT NOT NULL,
  title TEXT,
  source_query TEXT NOT NULL DEFAULT '',
  UNIQUE(event_id, provider, canonical_url, relation, source_query)
);

CREATE INDEX IF NOT EXISTS idx_call_events_ts ON call_events(ts_ms);
CREATE INDEX IF NOT EXISTS idx_call_events_server_ts ON call_events(server, ts_ms);
CREATE INDEX IF NOT EXISTS idx_call_events_tool_ts ON call_events(tool, ts_ms);
CREATE INDEX IF NOT EXISTS idx_call_events_session_ts ON call_events(session_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_call_event_targets_server_tool ON call_event_targets(server, tool);
CREATE INDEX IF NOT EXISTS idx_forwarded_header_usage_ts ON forwarded_header_usage(ts_ms);
CREATE INDEX IF NOT EXISTS idx_forwarded_header_usage_server ON forwarded_header_usage(server, header_name, ts_ms);
CREATE INDEX IF NOT EXISTS idx_research_queries_provider_query ON research_queries(provider, query);
CREATE INDEX IF NOT EXISTS idx_web_page_observations_domain ON web_page_observations(domain, path);
CREATE INDEX IF NOT EXISTS idx_web_page_observations_url ON web_page_observations(canonical_url);

CREATE VIEW IF NOT EXISTS audit_forwarded_headers AS
SELECT
  fh.ts,
  fh.ts_ms,
  fh.server,
  fh.tool,
  COALESCE(fh.session_id, '') AS session_id,
  COALESCE(fh.principal, '') AS principal,
  fh.header_name
FROM forwarded_header_usage fh;
`;

function numberOr(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function integerOr(value: number | undefined, fallback = 0): number {
  return Number.isFinite(value) ? Math.max(0, Math.round(value as number)) : fallback;
}

function textOr(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function jsonArray(value: unknown): unknown[] {
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function projectArray(value: unknown): Array<{ name: string; path: string }> {
  return jsonArray(value).flatMap((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const record = item as Record<string, unknown>;
    const name = textOr(record.name);
    const path = textOr(record.path);
    return name && path ? [{ name, path }] : [];
  });
}

function serializeArguments(value: unknown): string | null {
  if (value === undefined) return null;
  try {
    return JSON.stringify(value) ?? null;
  } catch {
    return null;
  }
}

function rowToBreakdown(row: Record<string, unknown>): EventStoreBreakdownRow {
  return {
    name: textOr(row.name, "(unknown)"),
    calls: numberOr(row.calls),
    errors: numberOr(row.errors),
    avgDurationMs: numberOr(row.avgDurationMs),
    bytesIn: numberOr(row.bytesIn),
    bytesOut: numberOr(row.bytesOut),
    lastCallAt: textOr(row.lastCallAt),
  };
}

class EventStoreEngine {
  private readonly db: DatabaseSync;
  private readonly maxRows: number;
  private readonly retentionMs: number;
  private readonly pruneEvery: number;
  private readonly now: () => number;
  private insertsSincePrune = 0;

  private readonly insertEvent: StatementSync;
  private readonly insertTarget: StatementSync;
  private readonly insertForwardedHeader: StatementSync;
  private readonly insertResearchQuery: StatementSync;
  private readonly insertWebPageObservation: StatementSync;
  private readonly pruneAgeStmt: StatementSync;
  private readonly pruneRowsStmt: StatementSync;
  private readonly totalsStmt: StatementSync;
  private readonly serverBreakdownStmt: StatementSync;
  private readonly toolBreakdownStmt: StatementSync;
  private readonly sessionBreakdownStmt: StatementSync;
  private readonly transportBreakdownStmt: StatementSync;
  private readonly forwardedHeaderStmt: StatementSync;

  constructor(options: EventStoreOptions, Database: DatabaseSyncConstructor) {
    mkdirSync(dirname(options.path), { recursive: true });
    this.maxRows = options.maxRows ?? DEFAULT_EVENT_STORE_MAX_ROWS;
    this.retentionMs = (options.retentionDays ?? DEFAULT_EVENT_STORE_RETENTION_DAYS) * 24 * 60 * 60_000;
    this.pruneEvery = options.pruneEvery ?? DEFAULT_EVENT_STORE_PRUNE_EVERY;
    this.now = options.now ?? Date.now;
    this.db = new Database(options.path);
    this.db.exec(SCHEMA_SQL);
    this.migrateCallEventColumns();
    this.insertEvent = this.db.prepare(`
      INSERT INTO call_events (
        ts_ms, ts, server, tool, arguments_json, target_tool, session_id,
        agent_signature, project_name, project_path, principal, transport, duration_ms,
        ok, status, error_class, bytes_in, bytes_out, cache_hit, tool_kind,
        operation, downstream_calls
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.insertTarget = this.db.prepare(`
      INSERT INTO call_event_targets (event_id, server, tool, count)
      VALUES (?, ?, ?, ?)
    `);
    this.insertForwardedHeader = this.db.prepare(`
      INSERT INTO forwarded_header_usage (
        event_id, ts_ms, ts, server, tool, session_id, principal, header_name
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.insertResearchQuery = this.db.prepare(`
      INSERT OR IGNORE INTO research_queries (event_id, provider, query)
      VALUES (?, ?, ?)
    `);
    this.insertWebPageObservation = this.db.prepare(`
      INSERT OR IGNORE INTO web_page_observations (
        event_id, provider, url, canonical_url, domain, path, relation, title, source_query
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.pruneAgeStmt = this.db.prepare("DELETE FROM call_events WHERE ts_ms < ?");
    this.pruneRowsStmt = this.db.prepare(`
      DELETE FROM call_events
      WHERE id IN (
        SELECT id FROM call_events
        ORDER BY ts_ms DESC, id DESC
        LIMIT -1 OFFSET ?
      )
    `);
    this.totalsStmt = this.db.prepare(`
      SELECT
        COUNT(*) AS calls,
        COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(ROUND(AVG(duration_ms)), 0) AS avgDurationMs,
        COALESCE(SUM(bytes_in), 0) AS bytesIn,
        COALESCE(SUM(bytes_out), 0) AS bytesOut
      FROM call_events
      WHERE ts_ms >= ? AND ts_ms <= ?
    `);
    this.serverBreakdownStmt = this.db.prepare(`
      WITH server_events AS (
        SELECT DISTINCT
          t.server AS server,
          e.id AS event_id,
          e.ok AS ok,
          e.duration_ms AS duration_ms,
          e.bytes_in AS bytes_in,
          e.bytes_out AS bytes_out,
          e.ts AS ts
        FROM call_events e
        JOIN call_event_targets t ON t.event_id = e.id
        WHERE e.ts_ms >= ? AND e.ts_ms <= ? AND t.server IS NOT NULL
      )
      SELECT
        COALESCE(NULLIF(server, ''), '(unknown)') AS name,
        COUNT(*) AS calls,
        COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(ROUND(AVG(duration_ms)), 0) AS avgDurationMs,
        COALESCE(SUM(bytes_in), 0) AS bytesIn,
        COALESCE(SUM(bytes_out), 0) AS bytesOut,
        MAX(ts) AS lastCallAt
      FROM server_events
      GROUP BY server
      ORDER BY calls DESC, name ASC
      LIMIT ?
    `);
    this.toolBreakdownStmt = this.db.prepare(`
      SELECT
        COALESCE(NULLIF(t.tool, ''), e.tool) AS name,
        COUNT(DISTINCT e.id) AS calls,
        COALESCE(SUM(CASE WHEN e.ok = 0 THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(ROUND(AVG(e.duration_ms)), 0) AS avgDurationMs,
        COALESCE(SUM(e.bytes_in), 0) AS bytesIn,
        COALESCE(SUM(e.bytes_out), 0) AS bytesOut,
        MAX(e.ts) AS lastCallAt
      FROM call_events e
      LEFT JOIN call_event_targets t ON t.event_id = e.id
      WHERE e.ts_ms >= ? AND e.ts_ms <= ?
      GROUP BY name
      ORDER BY calls DESC, name ASC
      LIMIT ?
    `);
    this.sessionBreakdownStmt = this.db.prepare(`
      SELECT
        COALESCE(NULLIF(session_id, ''), '(none)') AS name,
        COUNT(*) AS calls,
        COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(ROUND(AVG(duration_ms)), 0) AS avgDurationMs,
        COALESCE(SUM(bytes_in), 0) AS bytesIn,
        COALESCE(SUM(bytes_out), 0) AS bytesOut,
        MAX(ts) AS lastCallAt
      FROM call_events
      WHERE ts_ms >= ? AND ts_ms <= ?
      GROUP BY COALESCE(NULLIF(session_id, ''), '(none)')
      ORDER BY calls DESC, name ASC
      LIMIT ?
    `);
    this.transportBreakdownStmt = this.db.prepare(`
      SELECT
        COALESCE(NULLIF(transport, ''), 'mcp') AS name,
        COUNT(*) AS calls,
        COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS errors,
        COALESCE(ROUND(AVG(duration_ms)), 0) AS avgDurationMs,
        COALESCE(SUM(bytes_in), 0) AS bytesIn,
        COALESCE(SUM(bytes_out), 0) AS bytesOut,
        MAX(ts) AS lastCallAt
      FROM call_events
      WHERE ts_ms >= ? AND ts_ms <= ?
      GROUP BY COALESCE(NULLIF(transport, ''), 'mcp')
      ORDER BY calls DESC, name ASC
      LIMIT ?
    `);
    this.forwardedHeaderStmt = this.db.prepare(`
      SELECT
        server,
        tool,
        COALESCE(NULLIF(session_id, ''), '(none)') AS sessionId,
        COALESCE(NULLIF(principal, ''), '(anonymous)') AS principal,
        header_name AS headerName,
        COUNT(*) AS calls,
        MAX(ts) AS lastSeenAt
      FROM audit_forwarded_headers
      WHERE ts_ms >= ? AND ts_ms <= ?
      GROUP BY server, tool, sessionId, principal, header_name
      ORDER BY calls DESC, lastSeenAt DESC
      LIMIT ?
    `);
    this.backfillResearchObservations();
  }

  /** `CREATE TABLE IF NOT EXISTS` doesn't add columns to a table that already existed on disk. */
  private migrateCallEventColumns(): void {
    const columns = this.db.prepare("PRAGMA table_info(call_events)").all();
    const names = new Set(columns.map((column) => String(column.name)));
    if (!names.has("transport")) {
      this.db.exec("ALTER TABLE call_events ADD COLUMN transport TEXT");
    }
    if (!names.has("arguments_json")) {
      this.db.exec("ALTER TABLE call_events ADD COLUMN arguments_json TEXT");
    }
    if (!names.has("agent_signature")) {
      this.db.exec("ALTER TABLE call_events ADD COLUMN agent_signature TEXT");
    }
    if (!names.has("project_name")) {
      this.db.exec("ALTER TABLE call_events ADD COLUMN project_name TEXT");
    }
    if (!names.has("project_path")) {
      this.db.exec("ALTER TABLE call_events ADD COLUMN project_path TEXT");
    }
    // Builds before 0.24.2 classified downstream tool failures separately but
    // accidentally persisted them as ok=1. Repair historical rows on open so
    // drill-down totals and the raw database agree with current semantics.
    this.db.prepare(`
      UPDATE call_events
      SET ok = 0
      WHERE status = 'downstream_error' AND ok <> 0
    `).run();
  }

  recordCalls(samples: EventStoreCallSample[]): void {
    if (samples.length === 0) return;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const sample of samples) this.insertCall(sample);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }

    this.insertsSincePrune += samples.length;
    if (this.insertsSincePrune >= this.pruneEvery) {
      this.prune();
    }
  }

  private insertCall(sample: EventStoreCallSample): void {
    const tsMs = sample.timestampMs ?? this.now();
    const ts = new Date(tsMs).toISOString();
    const targets = this.normalizeTargets(sample);
    const forwardedHeaders = [...new Set((sample.forwardedHeaders ?? []).map((h) => h.toLowerCase()))]
      .filter(Boolean)
      .sort();
    const result = this.insertEvent.run(
      tsMs,
      ts,
      sample.server ?? null,
      sample.tool,
      serializeArguments(sample.arguments),
      sample.targetTool ?? null,
      sample.sessionId ?? null,
      sample.agentSignature ?? null,
      sample.projectName ?? null,
      sample.projectPath ?? null,
      sample.principal ?? null,
      sample.transport ?? null,
      integerOr(sample.durationMs),
      sample.ok ? 1 : 0,
      sample.status ?? null,
      sample.errorClass ?? null,
      integerOr(sample.bytesIn),
      integerOr(sample.bytesOut),
      sample.cacheHit ? 1 : 0,
      sample.toolKind ?? null,
      sample.operation ?? null,
      integerOr(sample.downstreamCalls)
    );
    const eventId = Number(result.lastInsertRowid);
    for (const target of targets) {
      this.insertTarget.run(eventId, target.server ?? null, target.tool, integerOr(target.count, 1));
    }
    for (const target of targets) {
      if (!target.server) continue;
      for (const header of forwardedHeaders) {
        this.insertForwardedHeader.run(
          eventId,
          tsMs,
          ts,
          target.server,
          target.tool,
          sample.sessionId ?? null,
          sample.principal ?? null,
          header
        );
      }
    }
    this.insertResearchObservations(eventId, sample.research ?? {
      queries: [],
      pages: [],
    });
  }

  private insertResearchObservations(
    eventId: number,
    observations: ResearchObservations
  ): void {
    for (const observation of observations.queries) {
      this.insertResearchQuery.run(
        eventId,
        observation.provider,
        observation.query
      );
    }
    for (const observation of observations.pages) {
      this.insertWebPageObservation.run(
        eventId,
        observation.provider,
        observation.url,
        observation.canonicalUrl,
        observation.domain,
        observation.path,
        observation.relation,
        observation.title ?? null,
        observation.query ?? ""
      );
    }
  }

  /** Populate the index from pre-index call arguments without inventing search-result visits. */
  private backfillResearchObservations(): void {
    const rows = this.db.prepare(`
      SELECT e.id, e.server, e.tool, e.arguments_json
      FROM call_events e
      WHERE e.ok = 1
        AND e.arguments_json IS NOT NULL
        AND (
          e.tool LIKE '%web_search_exa'
          OR e.tool LIKE '%web_fetch_exa'
          OR e.tool LIKE '%web_search_sogou'
          OR e.tool LIKE '%web_fetch_sogou'
          OR e.tool LIKE '%batch_research_sogou'
          OR e.server LIKE 'qcc_%'
          OR e.tool LIKE 'qcc_%__%'
          OR e.tool IN ('callmux_call', 'callmux_parallel', 'callmux_batch', 'callmux_pipeline')
        )
    `).all();
    this.db.exec("BEGIN IMMEDIATE");
    try {
      for (const row of rows) {
        let args: unknown;
        try {
          args = JSON.parse(textOr(row.arguments_json, "null"));
        } catch {
          continue;
        }
        this.insertResearchObservations(
          numberOr(row.id),
          extractResearchObservations(textOr(row.tool), args, undefined, textOr(row.server))
        );
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  queryDrilldown(options: {
    fromMs?: number;
    toMs?: number;
    limit?: number;
  } = {}): EventStoreDrilldown {
    const toMs = options.toMs ?? this.now();
    const fromMs = options.fromMs ?? toMs - 60 * 60_000;
    const limit = Math.max(1, Math.min(500, Math.round(options.limit ?? 25)));
    const totals = this.totalsStmt.get(fromMs, toMs) ?? {};
    return {
      totals: {
        calls: numberOr(totals.calls),
        errors: numberOr(totals.errors),
        avgDurationMs: numberOr(totals.avgDurationMs),
        bytesIn: numberOr(totals.bytesIn),
        bytesOut: numberOr(totals.bytesOut),
      },
      byServer: this.serverBreakdownStmt.all(fromMs, toMs, limit).map(rowToBreakdown),
      byTool: this.toolBreakdownStmt.all(fromMs, toMs, limit).map(rowToBreakdown),
      bySession: this.sessionBreakdownStmt.all(fromMs, toMs, limit).map(rowToBreakdown),
      byTransport: this.transportBreakdownStmt.all(fromMs, toMs, limit).map(rowToBreakdown),
      forwardedHeaders: this.forwardedHeaderStmt.all(fromMs, toMs, limit).map((row) => ({
        server: textOr(row.server),
        tool: textOr(row.tool),
        sessionId: textOr(row.sessionId),
        principal: textOr(row.principal),
        headerName: textOr(row.headerName),
        calls: numberOr(row.calls),
        lastSeenAt: textOr(row.lastSeenAt),
      })),
    };
  }

  queryResearchIndex(options: {
    fromMs?: number;
    toMs?: number;
    limit?: number;
    project?: string;
    signature?: string;
  } = {}): EventStoreResearchIndex {
    const toMs = options.toMs ?? this.now();
    const fromMs = options.fromMs ?? 0;
    const limit = Math.max(1, Math.min(500, Math.round(options.limit ?? 100)));
    const project = options.project?.trim() ?? "";
    const signature = options.signature?.trim() ?? "";
    const queryRows = this.db.prepare(`
      SELECT
        q.provider AS provider,
        q.query AS query,
        COUNT(DISTINCT q.event_id) AS calls,
        MIN(e.ts) AS firstSeenAt,
        MAX(e.ts) AS lastSeenAt,
        COALESCE(
          json_group_array(DISTINCT e.agent_signature)
            FILTER (WHERE e.agent_signature IS NOT NULL AND e.agent_signature <> ''),
          '[]'
        ) AS signaturesJson,
        COALESCE(
          json_group_array(DISTINCT json_object('name', e.project_name, 'path', e.project_path))
            FILTER (WHERE e.project_name IS NOT NULL AND e.project_path IS NOT NULL),
          '[]'
        ) AS projectsJson
      FROM research_queries q
      JOIN call_events e ON e.id = q.event_id
      WHERE e.ts_ms >= ? AND e.ts_ms <= ?
        AND (? = '' OR e.agent_signature = ?)
        AND (? = '' OR e.project_name = ? OR e.project_path = ?)
      GROUP BY q.provider, q.query
    `).all(fromMs, toMs, signature, signature, project, project, project).map((row) => ({
      provider: textOr(row.provider) as ResearchProvider,
      query: textOr(row.query),
      calls: numberOr(row.calls),
      firstSeenAt: textOr(row.firstSeenAt),
      lastSeenAt: textOr(row.lastSeenAt),
      signatures: jsonArray(row.signaturesJson).map(String).sort(),
      projects: projectArray(row.projectsJson),
    }));

    const aggregates = new Map<string, {
      provider: ResearchProvider;
      url: string;
      canonicalUrl: string;
      domain: string;
      path: string;
      title?: string;
      discoveries: number;
      fetches: number;
      queries: Set<string>;
      signatures: Set<string>;
      projects: Map<string, { name: string; path: string }>;
      firstSeenAt: string;
      lastSeenAt: string;
    }>();
    const pageRows = this.db.prepare(`
      SELECT
        p.provider, p.url, p.canonical_url, p.domain, p.path,
        p.relation, p.title, p.source_query, e.ts,
        e.agent_signature, e.project_name, e.project_path
      FROM web_page_observations p
      JOIN call_events e ON e.id = p.event_id
      WHERE e.ts_ms >= ? AND e.ts_ms <= ?
        AND (? = '' OR e.agent_signature = ?)
        AND (? = '' OR e.project_name = ? OR e.project_path = ?)
      ORDER BY e.ts ASC, p.id ASC
    `).all(fromMs, toMs, signature, signature, project, project, project);
    for (const row of pageRows) {
      const provider = textOr(row.provider) as ResearchProvider;
      const canonicalUrl = textOr(row.canonical_url);
      const key = `${provider}\0${canonicalUrl}`;
      let aggregate = aggregates.get(key);
      if (!aggregate) {
        aggregate = {
          provider,
          url: textOr(row.url),
          canonicalUrl,
          domain: textOr(row.domain),
          path: textOr(row.path, "/"),
          discoveries: 0,
          fetches: 0,
          queries: new Set(),
          signatures: new Set(),
          projects: new Map(),
          firstSeenAt: textOr(row.ts),
          lastSeenAt: textOr(row.ts),
        };
        aggregates.set(key, aggregate);
      }
      aggregate.url = textOr(row.url, aggregate.url);
      if (textOr(row.title)) aggregate.title = textOr(row.title);
      if (textOr(row.source_query)) aggregate.queries.add(textOr(row.source_query));
      if (textOr(row.agent_signature)) aggregate.signatures.add(textOr(row.agent_signature));
      if (textOr(row.project_name) && textOr(row.project_path)) {
        aggregate.projects.set(textOr(row.project_path), {
          name: textOr(row.project_name),
          path: textOr(row.project_path),
        });
      }
      if (textOr(row.relation) === "discovered") aggregate.discoveries += 1;
      if (textOr(row.relation) === "fetched") aggregate.fetches += 1;
      aggregate.lastSeenAt = textOr(row.ts, aggregate.lastSeenAt);
    }

    return buildResearchIndex(
      queryRows,
      [...aggregates.values()].map((row) => ({
        ...row,
        queries: [...row.queries].sort(),
        signatures: [...row.signatures].sort(),
        projects: [...row.projects.values()].sort((left, right) => left.name.localeCompare(right.name)),
      })),
      limit
    );
  }

  prune(now: number = this.now()): void {
    this.insertsSincePrune = 0;
    if (this.retentionMs > 0) {
      this.pruneAgeStmt.run(now - this.retentionMs);
    }
    if (this.maxRows > 0) {
      this.pruneRowsStmt.run(this.maxRows);
    }
  }

  checkpoint(): void {
    this.db.exec("PRAGMA wal_checkpoint(PASSIVE)");
  }

  close(): void {
    this.prune();
    this.checkpoint();
    this.db.close();
  }

  private normalizeTargets(sample: EventStoreCallSample): EventTargetSample[] {
    const targets = new Map<string, EventTargetSample>();
    const addTarget = (target: EventTargetSample) => {
      const tool = target.tool || sample.targetTool || sample.tool;
      const key = `${target.server ?? ""}\0${tool}`;
      const existing = targets.get(key);
      if (existing) {
        existing.count = (existing.count ?? 0) + (target.count ?? 1);
      } else {
        targets.set(key, {
          ...(target.server ? { server: target.server } : {}),
          tool,
          count: target.count ?? 1,
        });
      }
    };

    for (const target of sample.targets ?? []) {
      addTarget(target);
    }
    if (targets.size === 0) {
      addTarget({
        ...(sample.server ? { server: sample.server } : {}),
        tool: sample.targetTool ?? sample.tool,
        count: Math.max(1, sample.downstreamCalls ?? 1),
      });
    }

    return [...targets.values()];
  }
}

const EVENT_STORE_WORKER_MARKER = "callmux-event-store-worker";
const EVENT_STORE_BATCH_SIZE = 100;
const EVENT_STORE_BATCH_DELAY_MS = 20;
const EVENT_STORE_MAX_PENDING_SAMPLES = 10_000;

type WorkerCommand =
  | { id: number; type: "record"; samples: EventStoreCallSample[] }
  | { id: number; type: "query"; options: { fromMs?: number; toMs?: number; limit?: number } }
  | { id: number; type: "research-index"; options: { fromMs?: number; toMs?: number; limit?: number; project?: string; signature?: string } }
  | { id: number; type: "close" };

type WorkerReply =
  | { type: "ready" }
  | { id: number; ok: true; result?: unknown }
  | { id: number; ok: false; error: string };

interface EventStoreWorkerData {
  marker: typeof EVENT_STORE_WORKER_MARKER;
  options: Omit<EventStoreOptions, "now" | "onError"> & { nowMs?: number };
}

/**
 * Async facade over the synchronous node:sqlite implementation. Writes are
 * batched and every database operation runs in a dedicated worker, so request
 * completion and dashboard queries never block the listener event loop.
 */
export class EventStore {
  private readonly worker: Worker;
  private readonly now: () => number;
  private readonly onError: (error: Error) => void;
  private readonly pendingRequests = new Map<
    number,
    { resolve: (value: unknown) => void; reject: (error: Error) => void }
  >();
  private nextRequestId = 1;
  private pendingSamples: EventStoreCallSample[] = [];
  private droppedSamples = 0;
  private batchInFlight = false;
  private batchTimer: ReturnType<typeof setTimeout> | undefined;
  private flushWaiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = [];
  private closed = false;
  private terminalError: Error | undefined;
  private readonly readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private rejectReady!: (error: Error) => void;

  constructor(options: EventStoreOptions) {
    this.now = options.now ?? Date.now;
    this.onError = options.onError ?? (() => undefined);
    const workerOptions: EventStoreWorkerData["options"] = {
      path: options.path,
      ...(options.maxRows !== undefined ? { maxRows: options.maxRows } : {}),
      ...(options.retentionDays !== undefined ? { retentionDays: options.retentionDays } : {}),
      ...(options.pruneEvery !== undefined ? { pruneEvery: options.pruneEvery } : {}),
      ...(options.now ? { nowMs: options.now() } : {}),
    };
    this.readyPromise = new Promise<void>((resolve, reject) => {
      this.resolveReady = resolve;
      this.rejectReady = reject;
    });
    this.worker = new Worker(new URL(import.meta.url), {
      workerData: {
        marker: EVENT_STORE_WORKER_MARKER,
        options: workerOptions,
      } satisfies EventStoreWorkerData,
    });
    this.worker.on("message", (message: WorkerReply) => this.onWorkerMessage(message));
    this.worker.on("error", (error) => this.fail(error));
    this.worker.on("exit", (code) => {
      if (!this.closed) {
        this.fail(new Error(`event store worker exited with code ${code}`));
      }
    });
  }

  async ready(): Promise<void> {
    await this.readyPromise;
  }

  recordCall(sample: EventStoreCallSample): void {
    if (this.closed || this.terminalError) return;
    const normalized = {
      ...sample,
      timestampMs: sample.timestampMs ?? this.now(),
    };
    if (this.pendingSamples.length >= EVENT_STORE_MAX_PENDING_SAMPLES) {
      this.pendingSamples.shift();
      this.droppedSamples += 1;
      if (this.droppedSamples === 1 || this.droppedSamples % 1_000 === 0) {
        this.reportError(new Error(
          `event store queue capacity reached; dropped ${this.droppedSamples} call sample(s)`
        ));
      }
    }
    this.pendingSamples.push(normalized);
    if (this.pendingSamples.length >= EVENT_STORE_BATCH_SIZE) {
      this.scheduleBatch(0);
    } else if (!this.batchTimer && !this.batchInFlight) {
      this.scheduleBatch(EVENT_STORE_BATCH_DELAY_MS);
    }
  }

  async flush(): Promise<void> {
    await this.ready();
    if (this.terminalError) throw this.terminalError;
    if (this.pendingSamples.length === 0 && !this.batchInFlight) return;
    return await new Promise<void>((resolve, reject) => {
      this.flushWaiters.push({ resolve, reject });
      this.scheduleBatch(0);
    });
  }

  async queryDrilldown(options: {
    fromMs?: number;
    toMs?: number;
    limit?: number;
  } = {}): Promise<EventStoreDrilldown> {
    await this.flush();
    const normalized = {
      ...options,
      ...(options.toMs === undefined ? { toMs: this.now() } : {}),
    };
    return await this.request("query", { options: normalized }) as EventStoreDrilldown;
  }

  async queryResearchIndex(options: {
    fromMs?: number;
    toMs?: number;
    limit?: number;
    project?: string;
    signature?: string;
  } = {}): Promise<EventStoreResearchIndex> {
    await this.flush();
    const normalized = {
      ...options,
      ...(options.toMs === undefined ? { toMs: this.now() } : {}),
    };
    return await this.request("research-index", { options: normalized }) as EventStoreResearchIndex;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    try {
      await this.flush();
      await this.request("close", {});
    } finally {
      this.closed = true;
      if (this.batchTimer) clearTimeout(this.batchTimer);
      await this.worker.terminate();
    }
  }

  private scheduleBatch(delayMs: number): void {
    if (this.closed || this.terminalError || this.batchInFlight) return;
    if (this.batchTimer) clearTimeout(this.batchTimer);
    this.batchTimer = setTimeout(() => {
      this.batchTimer = undefined;
      void this.pumpBatch();
    }, delayMs);
    this.batchTimer.unref?.();
  }

  private async pumpBatch(): Promise<void> {
    if (this.batchInFlight || this.closed || this.terminalError) return;
    const samples = this.pendingSamples.splice(0, EVENT_STORE_BATCH_SIZE);
    if (samples.length === 0) {
      this.resolveFlushWaiters();
      return;
    }
    this.batchInFlight = true;
    try {
      await this.request("record", { samples });
    } catch (error) {
      this.reportError(error as Error);
      this.rejectFlushWaiters(error as Error);
    } finally {
      this.batchInFlight = false;
    }
    if (this.pendingSamples.length > 0) this.scheduleBatch(0);
    else this.resolveFlushWaiters();
  }

  private request(
    type: "record" | "query" | "research-index" | "close",
    payload: Record<string, unknown>
  ): Promise<unknown> {
    if (this.terminalError) return Promise.reject(this.terminalError);
    const id = this.nextRequestId++;
    return new Promise((resolve, reject) => {
      this.pendingRequests.set(id, { resolve, reject });
      try {
        this.worker.postMessage({ id, type, ...payload } as WorkerCommand);
      } catch (error) {
        // Structured-clone errors are synchronous. Remove the request here so
        // a malformed programmatic sample cannot leave a permanently pending
        // entry or make close()/flush() hang later.
        this.pendingRequests.delete(id);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private onWorkerMessage(message: WorkerReply): void {
    if ("type" in message && message.type === "ready") {
      this.resolveReady();
      return;
    }
    if (!("id" in message)) return;
    const pending = this.pendingRequests.get(message.id);
    if (!pending) return;
    this.pendingRequests.delete(message.id);
    if (message.ok) pending.resolve(message.result);
    else pending.reject(new Error(message.error));
  }

  private resolveFlushWaiters(): void {
    if (this.pendingSamples.length > 0 || this.batchInFlight) return;
    const waiters = this.flushWaiters.splice(0);
    for (const waiter of waiters) waiter.resolve();
  }

  private rejectFlushWaiters(error: Error): void {
    const waiters = this.flushWaiters.splice(0);
    for (const waiter of waiters) waiter.reject(error);
  }

  private fail(error: Error): void {
    if (this.terminalError) return;
    this.terminalError = error;
    this.rejectReady(error);
    this.reportError(error);
    for (const pending of this.pendingRequests.values()) pending.reject(error);
    this.pendingRequests.clear();
    this.rejectFlushWaiters(error);
  }

  private reportError(error: Error): void {
    try {
      this.onError(error);
    } catch {
      // Observability callbacks must not break the worker/queue state machine.
    }
  }
}

async function runEventStoreWorker(data: EventStoreWorkerData): Promise<void> {
  const port = parentPort;
  if (!port) return;
  try {
    const sqlite = await import("node:sqlite");
    const { nowMs, ...workerOptions } = data.options;
    const engine = new EventStoreEngine(
      {
        ...workerOptions,
        ...(nowMs !== undefined ? { now: () => nowMs } : {}),
      },
      sqlite.DatabaseSync
    );
    port.postMessage({ type: "ready" } satisfies WorkerReply);
    port.on("message", (command: WorkerCommand) => {
      try {
        if (command.type === "record") {
          engine.recordCalls(command.samples);
          port.postMessage({ id: command.id, ok: true } satisfies WorkerReply);
        } else if (command.type === "query") {
          port.postMessage({
            id: command.id,
            ok: true,
            result: engine.queryDrilldown(command.options),
          } satisfies WorkerReply);
        } else if (command.type === "research-index") {
          port.postMessage({
            id: command.id,
            ok: true,
            result: engine.queryResearchIndex(command.options),
          } satisfies WorkerReply);
        } else {
          engine.close();
          port.postMessage({ id: command.id, ok: true } satisfies WorkerReply);
        }
      } catch (error) {
        port.postMessage({
          id: command.id,
          ok: false,
          error: error instanceof Error ? error.message : String(error),
        } satisfies WorkerReply);
      }
    });
  } catch (error) {
    throw error;
  }
}

export async function openEventStore(options: EventStoreOptions): Promise<EventStore> {
  const store = new EventStore(options);
  await store.ready();
  return store;
}

if (!isMainThread && (workerData as EventStoreWorkerData | undefined)?.marker === EVENT_STORE_WORKER_MARKER) {
  void runEventStoreWorker(workerData as EventStoreWorkerData);
}
