import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";

export type ResearchProvider = "exa" | "sogou" | "qichacha";
export type WebPageRelation = "discovered" | "fetched";

export interface ResearchQueryObservation {
  provider: ResearchProvider;
  query: string;
}

export interface WebPageObservation {
  provider: ResearchProvider;
  url: string;
  canonicalUrl: string;
  domain: string;
  path: string;
  relation: WebPageRelation;
  title?: string;
  query?: string;
}

export interface ResearchObservations {
  queries: ResearchQueryObservation[];
  pages: WebPageObservation[];
}

export interface ResearchQueryAggregate extends ResearchQueryObservation {
  calls: number;
  firstSeenAt: string;
  lastSeenAt: string;
  signatures: string[];
  projects: ResearchProject[];
}

export interface ResearchProject {
  name: string;
  path: string;
}

export interface ResearchTermAggregate {
  provider: ResearchProvider;
  term: string;
  uses: number;
}

export interface WebPageAggregate {
  provider: ResearchProvider;
  url: string;
  canonicalUrl: string;
  domain: string;
  path: string;
  title?: string;
  discoveries: number;
  fetches: number;
  queries: string[];
  firstSeenAt: string;
  lastSeenAt: string;
  signatures: string[];
  projects: ResearchProject[];
}

export interface UrlTreeNode {
  segment: string;
  kind: "domain" | "path";
  prefix: string;
  depth: number;
  pages: number;
  discoveries: number;
  fetches: number;
  firstSeenAt: string;
  lastSeenAt: string;
  signatures: string[];
  projects: ResearchProject[];
  children: UrlTreeNode[];
}

export interface ResearchIndex {
  totals: {
    queries: number;
    uniqueQueries: number;
    pages: number;
    domains: number;
    discoveries: number;
    fetches: number;
  };
  topQueries: ResearchQueryAggregate[];
  topTerms: ResearchTermAggregate[];
  pages: WebPageAggregate[];
  urlTree: UrlTreeNode[];
}

interface Invocation {
  tool: string;
  arguments: Record<string, unknown>;
}

interface ResearchQueryRow extends ResearchQueryAggregate {}
interface WebPageRow extends WebPageAggregate {}

const TRACKING_PARAMETERS = new Set([
  "fbclid",
  "gclid",
  "mc_cid",
  "mc_eid",
  "ref_src",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function text(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : undefined;
}

function normalizedTool(tool: string): string {
  return tool.toLowerCase().replace(/^.*__/, "");
}

function providerForTool(
  tool: string,
  server?: string
): ResearchProvider | undefined {
  const qualified = tool.toLowerCase();
  if (server?.toLowerCase().startsWith("qcc_") || /^qcc_[a-z0-9_-]+__/.test(qualified)) {
    return "qichacha";
  }
  const name = normalizedTool(tool);
  if (name.endsWith("_exa")) return "exa";
  if (name.endsWith("_sogou")) return "sogou";
  return undefined;
}

const QICHACHA_QUERY_FIELDS = [
  "searchKey",
  "query",
  "keyword",
  "keywords",
  "personName",
  "name",
  "party",
  "regulationName",
  "caseNo",
] as const;

function qichachaQuery(args: Record<string, unknown>): string | undefined {
  const values: string[] = [];
  for (const field of QICHACHA_QUERY_FIELDS) {
    const raw = args[field];
    const candidates = Array.isArray(raw) ? raw : [raw];
    for (const candidate of candidates) {
      const value = text(candidate);
      if (value && !values.includes(value)) values.push(value);
    }
  }
  const query = values.join(" · ");
  return query ? query.slice(0, 500) : undefined;
}

function flattenInvocations(
  tool: string,
  value: unknown
): Invocation[] {
  const args = isRecord(value) ? value : {};
  const name = normalizedTool(tool);
  if (name === "callmux_call") {
    const childTool = text(args.tool);
    return childTool
      ? [{ tool: childTool, arguments: isRecord(args.arguments) ? args.arguments : {} }]
      : [];
  }
  if (name === "callmux_parallel" || name === "callmux_pipeline") {
    const values = name === "callmux_parallel" ? args.calls : args.steps;
    return Array.isArray(values)
      ? values.flatMap((item) => {
          if (!isRecord(item)) return [];
          const childTool = text(item.tool);
          return childTool
            ? [{
                tool: childTool,
                arguments: isRecord(item.arguments) ? item.arguments : {},
              }]
            : [];
        })
      : [];
  }
  if (name === "callmux_batch") {
    const childTool = text(args.tool);
    return childTool && Array.isArray(args.items)
      ? args.items.flatMap((item) => {
          if (!isRecord(item)) return [];
          return [{
            tool: childTool,
            arguments: isRecord(item.arguments) ? item.arguments : {},
          }];
        })
      : [];
  }
  return [{ tool, arguments: args }];
}

export function canonicalizeWebUrl(value: string): Omit<
  WebPageObservation,
  "provider" | "relation" | "title" | "query"
> | null {
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    url.username = "";
    url.password = "";
    url.hash = "";
    for (const key of [...url.searchParams.keys()]) {
      if (key.toLowerCase().startsWith("utm_") || TRACKING_PARAMETERS.has(key.toLowerCase())) {
        url.searchParams.delete(key);
      }
    }
    url.searchParams.sort();
    url.pathname = url.pathname.replace(/\/{2,}/g, "/");
    if (url.pathname.length > 1) url.pathname = url.pathname.replace(/\/+$/, "");
    return {
      url: value,
      canonicalUrl: url.toString(),
      domain: url.hostname.toLowerCase(),
      path: url.pathname || "/",
    };
  } catch {
    return null;
  }
}

function resultUrlCandidates(result: CallToolResult | undefined): Array<{
  url: string;
  title?: string;
  query?: string;
}> {
  if (!result || result.isError) return [];
  const candidates: Array<{ url: string; title?: string; query?: string }> = [];
  for (const part of result.content ?? []) {
    if (!("text" in part) || typeof part.text !== "string") continue;
    const lines = part.text.split(/\r?\n/);
    let title: string | undefined;
    for (const line of lines) {
      const titleMatch = line.match(/^Title:\s*(.+)$/i);
      if (titleMatch) {
        title = text(titleMatch[1]);
        continue;
      }
      const urlMatch = line.match(/^URL:\s*(https?:\/\/\S+)/i);
      if (urlMatch) {
        candidates.push({ url: urlMatch[1], ...(title ? { title } : {}) });
        title = undefined;
      }
    }
  }

  const visit = (value: unknown, depth: number): void => {
    if (depth > 8 || value === null || value === undefined) return;
    if (Array.isArray(value)) {
      for (const item of value) visit(item, depth + 1);
      return;
    }
    if (!isRecord(value)) return;
    const url = text(value.url);
    if (url?.startsWith("http")) {
      candidates.push({
        url,
        ...(text(value.title) ? { title: text(value.title) } : {}),
        ...(text(value.query) ? { query: text(value.query) } : {}),
      });
    }
    for (const child of Object.values(value)) visit(child, depth + 1);
  };
  visit(result.structuredContent, 0);
  return candidates;
}

export function extractResearchObservations(
  tool: string,
  args: unknown,
  result?: CallToolResult,
  server?: string
): ResearchObservations {
  const invocations = flattenInvocations(tool, args);
  const queries: ResearchQueryObservation[] = [];
  const pages: WebPageObservation[] = [];
  const searchInvocations: Invocation[] = [];
  const fetchInvocations: Invocation[] = [];

  const addPage = (
    provider: ResearchProvider,
    relation: WebPageRelation,
    rawUrl: string,
    title?: string,
    query?: string
  ) => {
    const normalized = canonicalizeWebUrl(rawUrl);
    if (!normalized) return;
    pages.push({
      provider,
      relation,
      ...normalized,
      ...(title ? { title } : {}),
      ...(query ? { query } : {}),
    });
  };

  for (const invocation of invocations) {
    const provider = providerForTool(invocation.tool, server);
    if (!provider) continue;
    if (provider === "qichacha") {
      const query = qichachaQuery(invocation.arguments);
      if (query) queries.push({ provider, query });
      continue;
    }
    const name = normalizedTool(invocation.tool);
    if (name === "web_search_exa" || name === "web_search_sogou") {
      const query = text(invocation.arguments.query);
      if (query) queries.push({ provider, query });
      searchInvocations.push(invocation);
    } else if (name === "batch_research_sogou") {
      for (const query of Array.isArray(invocation.arguments.queries)
        ? invocation.arguments.queries
        : []) {
        const value = text(query);
        if (value) queries.push({ provider: "sogou", query: value });
      }
      searchInvocations.push(invocation);
      fetchInvocations.push(invocation);
    }

    if (name === "web_fetch_exa") {
      const urls = Array.isArray(invocation.arguments.urls)
        ? invocation.arguments.urls
        : [invocation.arguments.url];
      for (const url of urls) {
        const value = text(url);
        if (value) addPage("exa", "fetched", value);
      }
      fetchInvocations.push(invocation);
    } else if (name === "web_fetch_sogou") {
      const value = text(invocation.arguments.result);
      if (value?.startsWith("http")) addPage("sogou", "fetched", value);
      fetchInvocations.push(invocation);
    }
  }

  if (searchInvocations.length === 0 && fetchInvocations.length === 0) {
    return {
      queries: [...new Map(queries.map((item) => [`${item.provider}\0${item.query}`, item])).values()],
      pages: [],
    };
  }

  const resultCandidates = resultUrlCandidates(result);
  const containsBatchResearch = invocations.some(
    (invocation) => normalizedTool(invocation.tool) === "batch_research_sogou"
  );
  const providers = new Set(
    [...searchInvocations, ...fetchInvocations]
      .map((invocation) => providerForTool(invocation.tool))
      .filter((provider): provider is ResearchProvider => provider !== undefined)
  );
  const resultRelation = containsBatchResearch
    ? "fetched"
    : fetchInvocations.length > 0 && searchInvocations.length === 0
    ? "fetched"
    : searchInvocations.length > 0 && fetchInvocations.length === 0
      ? "discovered"
      : undefined;
  const resultProvider = providers.size === 1 ? [...providers][0] : undefined;
  const soleQuery = queries.length === 1 ? queries[0].query : undefined;
  if (resultRelation && resultProvider) {
    for (const candidate of resultCandidates) {
      addPage(
        resultProvider,
        resultRelation,
        candidate.url,
        candidate.title,
        candidate.query ?? soleQuery
      );
    }
  }

  return {
    queries: [...new Map(queries.map((item) => [`${item.provider}\0${item.query}`, item])).values()],
    pages: [...new Map(pages.map((item) => [
      `${item.provider}\0${item.relation}\0${item.canonicalUrl}\0${item.query ?? ""}`,
      item,
    ])).values()],
  };
}

function queryTerms(query: string): string[] {
  const segmenter = new Intl.Segmenter("zh", { granularity: "word" });
  return [...segmenter.segment(query)]
    .filter((item) => item.isWordLike)
    .map((item) => item.segment.trim().toLocaleLowerCase())
    .filter((item) => item.length > 0 && !/^\d+$/.test(item));
}

interface MutableTreeNode extends Omit<
  UrlTreeNode,
  "children" | "pages" | "signatures" | "projects"
> {
  pageUrls: Set<string>;
  signatureSet: Set<string>;
  projectMap: Map<string, ResearchProject>;
  children: Map<string, MutableTreeNode>;
}

function buildUrlTree(pages: WebPageRow[]): UrlTreeNode[] {
  const roots = new Map<string, MutableTreeNode>();
  const touch = (node: MutableTreeNode, page: WebPageRow) => {
    node.pageUrls.add(page.canonicalUrl);
    node.discoveries += page.discoveries;
    node.fetches += page.fetches;
    for (const signature of page.signatures) node.signatureSet.add(signature);
    for (const project of page.projects) node.projectMap.set(project.path, project);
    if (!node.firstSeenAt || page.firstSeenAt < node.firstSeenAt) node.firstSeenAt = page.firstSeenAt;
    if (!node.lastSeenAt || page.lastSeenAt > node.lastSeenAt) node.lastSeenAt = page.lastSeenAt;
  };
  const displaySegment = (segment: string): string => {
    try {
      return decodeURIComponent(segment);
    } catch {
      return segment;
    }
  };

  for (const page of pages) {
    const existingRoot = roots.get(page.domain);
    let node: MutableTreeNode;
    if (existingRoot) {
      node = existingRoot;
    } else {
      node = {
        segment: page.domain,
        kind: "domain",
        prefix: page.domain,
        depth: 0,
        pageUrls: new Set(),
        discoveries: 0,
        fetches: 0,
        firstSeenAt: "",
        lastSeenAt: "",
        children: new Map(),
        signatureSet: new Set(),
        projectMap: new Map(),
      };
      roots.set(page.domain, node);
    }
    touch(node, page);
    const segments = page.path.split("/").filter(Boolean);
    let prefix = "";
    for (const segment of segments) {
      prefix += `/${segment}`;
      let child: MutableTreeNode | undefined = node.children.get(segment);
      if (!child) {
        child = {
          segment: displaySegment(segment),
          kind: "path",
          prefix: `${page.domain}${prefix}`,
          depth: node.depth + 1,
          pageUrls: new Set(),
          discoveries: 0,
          fetches: 0,
          firstSeenAt: "",
          lastSeenAt: "",
          children: new Map(),
          signatureSet: new Set(),
          projectMap: new Map(),
        };
        node.children.set(segment, child);
      }
      touch(child, page);
      node = child;
    }
  }

  const materialize = (node: MutableTreeNode): UrlTreeNode => ({
    segment: node.segment,
    kind: node.kind,
    prefix: node.prefix,
    depth: node.depth,
    pages: node.pageUrls.size,
    discoveries: node.discoveries,
    fetches: node.fetches,
    firstSeenAt: node.firstSeenAt,
    lastSeenAt: node.lastSeenAt,
    signatures: [...node.signatureSet].sort(),
    projects: [...node.projectMap.values()].sort((left, right) => left.name.localeCompare(right.name)),
    children: [...node.children.values()]
      .map(materialize)
      .sort((left, right) => right.pages - left.pages || left.segment.localeCompare(right.segment)),
  });
  return [...roots.values()]
    .map(materialize)
    .sort((left, right) => right.pages - left.pages || left.segment.localeCompare(right.segment));
}

export function buildResearchIndex(
  queryRows: ResearchQueryRow[],
  pageRows: WebPageRow[],
  limit = 100
): ResearchIndex {
  const termCounts = new Map<string, ResearchTermAggregate>();
  for (const row of queryRows) {
    for (const term of queryTerms(row.query)) {
      const key = `${row.provider}\0${term}`;
      const existing = termCounts.get(key);
      if (existing) existing.uses += row.calls;
      else termCounts.set(key, { provider: row.provider, term, uses: row.calls });
    }
  }
  const pages = [...pageRows]
    .sort((left, right) => right.lastSeenAt.localeCompare(left.lastSeenAt))
    .slice(0, Math.max(1, limit));
  return {
    totals: {
      queries: queryRows.reduce((sum, row) => sum + row.calls, 0),
      uniqueQueries: queryRows.length,
      pages: new Set(pageRows.map((row) => row.canonicalUrl)).size,
      domains: new Set(pageRows.map((row) => row.domain)).size,
      discoveries: pageRows.reduce((sum, row) => sum + row.discoveries, 0),
      fetches: pageRows.reduce((sum, row) => sum + row.fetches, 0),
    },
    topQueries: [...queryRows]
      .sort((left, right) => right.calls - left.calls || right.lastSeenAt.localeCompare(left.lastSeenAt))
      .slice(0, Math.max(1, limit)),
    topTerms: [...termCounts.values()]
      .sort((left, right) => right.uses - left.uses || left.term.localeCompare(right.term))
      .slice(0, Math.max(1, limit)),
    pages,
    urlTree: buildUrlTree(pageRows),
  };
}
