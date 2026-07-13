import type { CallToolResult, Tool } from "@modelcontextprotocol/sdk/types.js";
import type { CacheEntry, CachePolicyConfig } from "./types.js";

export type ToolSafetyAnnotations = Pick<
  NonNullable<Tool["annotations"]>,
  "readOnlyHint" | "idempotentHint"
>;

function normalizeToolName(tool: string): string {
  const separator = tool.lastIndexOf("__");
  return separator === -1 ? tool : tool.slice(separator + 2);
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => stableValue(item));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, stableValue(nested)])
    );
  }

  return value;
}

// Cache policy patterns are config-fixed, but shouldCache() runs on every
// cache get/set. Memoize compilation so the hot path reuses RegExp objects
// instead of recompiling per call. Bounded by the number of distinct patterns.
const patternRegExpCache = new Map<string, RegExp>();

function patternToRegExp(pattern: string): RegExp {
  let compiled = patternRegExpCache.get(pattern);
  if (!compiled) {
    const escaped = pattern.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
    compiled = new RegExp(`^${escaped.replace(/\*/g, ".*")}$`);
    patternRegExpCache.set(pattern, compiled);
  }
  return compiled;
}

function matchesPolicy(patterns: string[], candidates: string[]): boolean {
  return patterns.some((pattern) => {
    const matcher = patternToRegExp(pattern);
    return candidates.some((candidate) => matcher.test(candidate));
  });
}

export class CallCache {
  private entries = new Map<string, CacheEntry>();
  private ttlMs: number;
  private maxEntries: number;
  private pruneIntervalMs: number;
  private hits = 0;
  private misses = 0;
  private nextPruneAt = 0;
  private globalPolicy?: CachePolicyConfig;
  private serverPolicies: Map<string, CachePolicyConfig>;

  constructor(
    ttlSeconds: number,
    globalPolicy?: CachePolicyConfig,
    serverPolicies?: Record<string, CachePolicyConfig | undefined>,
    maxEntries = 1000
  ) {
    this.ttlMs = ttlSeconds * 1000;
    this.maxEntries = maxEntries;
    this.pruneIntervalMs = this.ttlMs > 0 ? Math.min(this.ttlMs, 1_000) : 0;
    this.globalPolicy = globalPolicy;
    this.serverPolicies = new Map(
      Object.entries(serverPolicies ?? {}).filter(([, policy]) => policy !== undefined)
    ) as Map<string, CachePolicyConfig>;
  }

  private cacheCandidates(tool: string, server?: string): string[] {
    const candidates = new Set<string>();
    const normalized = normalizeToolName(tool);
    candidates.add(tool);
    candidates.add(normalized);

    if (server) {
      candidates.add(`${server}__${normalized}`);
    }

    return Array.from(candidates);
  }

  private effectiveServer(tool: string, server?: string): string | undefined {
    if (server) return server;
    const separator = tool.lastIndexOf("__");
    if (separator <= 0) return undefined;
    const inferred = tool.slice(0, separator);
    return this.serverPolicies.has(inferred) ? inferred : undefined;
  }

  private policyDecision(
    tool: string,
    server?: string
  ): { explicitlyAllowed: boolean; denied: boolean; hasAllowList: boolean } {
    const effectiveServer = this.effectiveServer(tool, server);
    const candidates = this.cacheCandidates(tool, effectiveServer);
    const policies = [
      this.globalPolicy,
      effectiveServer ? this.serverPolicies.get(effectiveServer) : undefined,
    ].filter((policy): policy is CachePolicyConfig => policy !== undefined);

    const denyPatterns = policies.flatMap((policy) => policy.denyTools ?? []);
    const allowPatterns = policies.flatMap((policy) => policy.allowTools ?? []);
    return {
      denied:
        denyPatterns.length > 0 && matchesPolicy(denyPatterns, candidates),
      explicitlyAllowed:
        allowPatterns.length > 0 && matchesPolicy(allowPatterns, candidates),
      hasAllowList: allowPatterns.length > 0,
    };
  }

  private shouldCache(
    tool: string,
    server?: string,
    annotations?: ToolSafetyAnnotations
  ): boolean {
    const policy = this.policyDecision(tool, server);
    if (policy.denied) return false;
    if (policy.explicitlyAllowed) return true;
    if (policy.hasAllowList) return false;

    // MCP annotations default to false. Unknown tools are deliberately unsafe:
    // names such as `get_and_delete` are not a trustworthy side-effect signal.
    return annotations?.readOnlyHint === true;
  }

  isSafeToRetry(
    tool: string,
    server?: string,
    annotations?: ToolSafetyAnnotations
  ): boolean {
    const policy = this.policyDecision(tool, server);
    // An explicit cache allow-list is also an operator assertion that identical
    // calls are replay-safe. Cache deny-lists do not imply the opposite: callers
    // may disable caching for freshness while an annotation still permits retry.
    return (
      policy.explicitlyAllowed ||
      annotations?.readOnlyHint === true ||
      annotations?.idempotentHint === true
    );
  }

  canCache(
    tool: string,
    server?: string,
    annotations?: ToolSafetyAnnotations
  ): boolean {
    if (this.ttlMs <= 0) return false;
    return this.shouldCache(tool, server, annotations);
  }

  private key(
    tool: string,
    args?: Record<string, unknown>,
    server?: string,
    scope?: string
  ): string {
    return JSON.stringify({
      server: server ?? null,
      scope: scope ?? null,
      tool,
      arguments: args === undefined ? null : stableValue(args),
    });
  }

  private pruneExpired(now = Date.now()): void {
    for (const [key, entry] of this.entries) {
      if (now > entry.expiresAt) {
        this.entries.delete(key);
      }
    }
  }

  private maybePruneExpired(now = Date.now()): void {
    if (this.pruneIntervalMs <= 0 || now < this.nextPruneAt) return;
    this.pruneExpired(now);
    this.nextPruneAt = now + this.pruneIntervalMs;
  }

  private evictOldest(): void {
    while (this.entries.size > this.maxEntries) {
      const oldest = this.entries.keys().next().value as string | undefined;
      if (oldest === undefined) return;
      this.entries.delete(oldest);
    }
  }

  get(
    tool: string,
    args?: Record<string, unknown>,
    server?: string,
    scope?: string,
    annotations?: ToolSafetyAnnotations
  ): CallToolResult | null {
    if (this.ttlMs <= 0) return null;
    const effectiveServer = this.effectiveServer(tool, server);
    if (!this.shouldCache(tool, effectiveServer, annotations)) return null;
    const now = Date.now();
    this.maybePruneExpired(now);

    const key = this.key(tool, args, effectiveServer, scope);
    const entry = this.entries.get(key);
    if (!entry) {
      this.misses++;
      return null;
    }
    if (now > entry.expiresAt) {
      this.entries.delete(key);
      this.misses++;
      return null;
    }

    this.entries.delete(key);
    this.entries.set(key, entry);
    this.hits++;
    return entry.result;
  }

  set(
    tool: string,
    args: Record<string, unknown> | undefined,
    result: CallToolResult,
    server?: string,
    scope?: string,
    annotations?: ToolSafetyAnnotations
  ): void {
    if (this.ttlMs <= 0) return;
    const effectiveServer = this.effectiveServer(tool, server);
    if (!this.shouldCache(tool, effectiveServer, annotations)) return;
    if (result.isError) return;
    const now = Date.now();
    this.maybePruneExpired(now);

    this.entries.set(this.key(tool, args, effectiveServer, scope), {
      tool,
      server: effectiveServer,
      result,
      expiresAt: now + this.ttlMs,
    });
    this.evictOldest();
  }

  invalidate(tool?: string, server?: string): void {
    this.pruneExpired();

    if (!tool) {
      if (!server) {
        this.entries.clear();
        return;
      }

      for (const [key, entry] of this.entries) {
        if (entry.server === server) {
          this.entries.delete(key);
        }
      }
      return;
    }

    for (const [key, entry] of this.entries) {
      if (entry.tool === tool && (server === undefined || entry.server === server)) {
        this.entries.delete(key);
      }
    }
  }

  get size(): number {
    this.pruneExpired();
    return this.entries.size;
  }

  get ttlSeconds(): number {
    return this.ttlMs / 1000;
  }

  stats(): {
    entries: number;
    ttlSeconds: number;
    enabled: boolean;
    maxEntries: number;
    hits: number;
    misses: number;
    hitRate: number;
  } {
    this.pruneExpired();
    const lookups = this.hits + this.misses;
    return {
      entries: this.entries.size,
      ttlSeconds: this.ttlMs / 1000,
      enabled: this.ttlMs > 0,
      maxEntries: this.maxEntries,
      hits: this.hits,
      misses: this.misses,
      hitRate: lookups > 0 ? this.hits / lookups : 0,
    };
  }
}
