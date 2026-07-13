import type { MetricsConfig } from "./types.js";

const DEFAULT_METRICS_PATH = "/metrics";
const DEFAULT_MAX_LABEL_SERIES = 256;
const UNKNOWN_ROUTE = "/__unmatched__";
const OVERFLOW_ROUTE = "/__overflow__";

interface CounterMap {
  [labelKey: string]: number;
}

function normalizePath(path: string): string {
  if (!path.startsWith("/")) return `/${path}`;
  return path;
}

function normalizeMethod(method: string): string {
  const normalized = method.toUpperCase();
  return /^(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)$/.test(normalized)
    ? normalized
    : "OTHER";
}

function normalizeStatus(status: number): string {
  return Number.isInteger(status) && status >= 100 && status <= 599
    ? String(status)
    : "other";
}

/**
 * Never let caller-controlled path segments become Prometheus labels. The
 * listener normally supplies an exact route template; this fallback keeps the
 * metrics class safe when used directly by integrations or older callers.
 */
function normalizeRoute(route: string | undefined): string {
  if (!route || !route.startsWith("/")) return UNKNOWN_ROUTE;
  // `labelKey` uses `|` as an internal separator, so reject it here along with
  // URL/query and control characters. This also keeps configured route labels
  // readable and prevents a direct integration from synthesizing labels.
  if (route.length > 160 || /[|?#\u0000-\u001f\u007f]/.test(route)) {
    return UNKNOWN_ROUTE;
  }
  return route;
}

function labelKey(labels: Record<string, string>): string {
  return Object.entries(labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${key}=${value}`)
    .join("|");
}

function parseLabelKey(value: string): Record<string, string> {
  if (value.length === 0) return {};
  const labels: Record<string, string> = {};
  for (const part of value.split("|")) {
    const eq = part.indexOf("=");
    if (eq <= 0) continue;
    labels[part.slice(0, eq)] = part.slice(eq + 1);
  }
  return labels;
}

function formatLabels(labels: Record<string, string>): string {
  const entries = Object.entries(labels);
  if (entries.length === 0) return "";
  const pairs = entries
    .sort(([a], [b]) => a.localeCompare(b))
    .map(
      ([key, value]) =>
        `${key}="${value
          .replace(/\\/g, "\\\\")
          .replace(/"/g, '\\"')
          .replace(/\n/g, "\\n")
          .replace(/\r/g, "\\r")}"`
    );
  return `{${pairs.join(",")}}`;
}

function normalizeConfig(config: MetricsConfig | undefined): Required<MetricsConfig> {
  return {
    enabled: config?.enabled ?? true,
    path: normalizePath(config?.path ?? DEFAULT_METRICS_PATH),
    allowUnauthenticated: config?.allowUnauthenticated ?? false,
  };
}

export class PrometheusMetrics {
  private config: Required<MetricsConfig>;
  private inflightRequests = 0;
  private requestsTotal: CounterMap = {};
  private requestDurationSecondsSum: CounterMap = {};
  private requestDurationSecondsCount: CounterMap = {};
  private readonly knownSeries = new Set<string>();

  constructor(config: MetricsConfig | undefined) {
    this.config = normalizeConfig(config);
  }

  isEnabled(): boolean {
    return this.config.enabled;
  }

  getPath(): string {
    return this.config.path;
  }

  allowUnauthenticated(): boolean {
    return this.config.allowUnauthenticated;
  }

  onRequestStart(): void {
    if (!this.config.enabled) return;
    this.inflightRequests += 1;
  }

  onRequestComplete(input: {
    method: string;
    path: string;
    /** A bounded route template such as /management/v1/servers/{server}. */
    route?: string;
    status: number;
    durationMs: number;
  }): void {
    if (!this.config.enabled) return;
    this.inflightRequests = Math.max(0, this.inflightRequests - 1);

    let labels = {
      method: normalizeMethod(input.method),
      path: normalizeRoute(input.route),
      status: normalizeStatus(input.status),
    };
    let key = labelKey(labels);
    if (!this.knownSeries.has(key)) {
      // Reserve the final slot for one overflow series so the total never
      // exceeds the configured cardinality ceiling.
      if (this.knownSeries.size >= DEFAULT_MAX_LABEL_SERIES - 1) {
        labels = {
          method: "OTHER",
          path: OVERFLOW_ROUTE,
          status: "other",
        };
        key = labelKey(labels);
      }
      this.knownSeries.add(key);
    }
    this.requestsTotal[key] = (this.requestsTotal[key] ?? 0) + 1;

    const durationSeconds = input.durationMs / 1000;
    this.requestDurationSecondsSum[key] =
      (this.requestDurationSecondsSum[key] ?? 0) + durationSeconds;
    this.requestDurationSecondsCount[key] =
      (this.requestDurationSecondsCount[key] ?? 0) + 1;
  }

  renderPrometheusText(): string {
    const lines: string[] = [];

    lines.push("# HELP callmux_http_inflight_requests Current in-flight HTTP requests");
    lines.push("# TYPE callmux_http_inflight_requests gauge");
    lines.push(`callmux_http_inflight_requests ${this.inflightRequests}`);

    lines.push("# HELP callmux_http_requests_total Total HTTP requests by method/path/status");
    lines.push("# TYPE callmux_http_requests_total counter");
    for (const [key, value] of Object.entries(this.requestsTotal)) {
      lines.push(
        `callmux_http_requests_total${formatLabels(parseLabelKey(key))} ${value}`
      );
    }

    lines.push(
      "# HELP callmux_http_request_duration_seconds_sum Total request duration seconds by method/path/status"
    );
    lines.push("# TYPE callmux_http_request_duration_seconds_sum counter");
    for (const [key, value] of Object.entries(this.requestDurationSecondsSum)) {
      lines.push(
        `callmux_http_request_duration_seconds_sum${formatLabels(
          parseLabelKey(key)
        )} ${value}`
      );
    }

    lines.push(
      "# HELP callmux_http_request_duration_seconds_count Total completed requests used for duration aggregation"
    );
    lines.push("# TYPE callmux_http_request_duration_seconds_count counter");
    for (const [key, value] of Object.entries(this.requestDurationSecondsCount)) {
      lines.push(
        `callmux_http_request_duration_seconds_count${formatLabels(
          parseLabelKey(key)
        )} ${value}`
      );
    }

    return `${lines.join("\n")}\n`;
  }
}
