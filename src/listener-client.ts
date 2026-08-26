import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { encodeCwdHeader } from "./cwd-header.js";
import { listenerUrls, parseHttpBody } from "./doctor.js";

const MCP_ACCEPT_HEADER = "application/json, text/event-stream";
const MCP_PROTOCOL_VERSION = "2025-11-25";
const CWD_HEADER = "x-callmux-cwd";
const CLIENT_HEADER = "x-callmux-client";
const CLIENT_INFO = { name: "callmux-cli", version: "1.0" };
const INITIALIZE_ID = 1;
const REQUEST_ID = 2;

export interface ListenerCallOptions {
  cwd?: string;
  headers?: Record<string, string>;
  timeoutMs?: number;
}

export interface ListenerRequestOutcome {
  ok: boolean;
  mcpUrl: string;
  sessionId?: string;
  httpStatus?: number;
  result?: unknown;
  error?: string;
}

export interface ListenerCallOutcome {
  ok: boolean;
  mcpUrl: string;
  sessionId?: string;
  httpStatus?: number;
  result?: CallToolResult;
  error?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function jsonRpcErrorMessage(body: unknown): string | undefined {
  if (!isRecord(body) || !isRecord(body.error)) return undefined;
  const message = body.error.message;
  return typeof message === "string" ? message : "JSON-RPC error";
}

/**
 * Generic authenticated JSON-RPC round-trip to a running callmux listener:
 * opens a short-lived MCP session (initialize + one request) over plain
 * fetch, best-effort terminates the session afterward, and maps HTTP/JSON-RPC
 * failures into a single outcome shape. No persistent SDK client/transport.
 * `callListenerTool` (tools/call) and later MCP2CLI phases (tools/list, ...)
 * are thin wrappers over this.
 */
export async function listenerRequest(
  url: string,
  method: string,
  params: Record<string, unknown>,
  options: ListenerCallOptions = {}
): Promise<ListenerRequestOutcome> {
  let mcpUrl: string;
  try {
    ({ mcpUrl } = listenerUrls(url));
  } catch (error) {
    // A malformed --url (e.g. "not-a-url") makes new URL() throw. Return it as a
    // usage-failure outcome so the CLI exits 2, instead of letting it propagate
    // to the top-level catch which would exit 1 (the tool-error code).
    return {
      ok: false,
      mcpUrl: url,
      error: `invalid listener URL "${url}": ${error instanceof Error ? error.message : String(error)}`,
    };
  }
  const baseHeaders = options.headers ?? {};
  const mcpHeaders: Record<string, string> = {
    ...baseHeaders,
    "Content-Type": "application/json",
    Accept: MCP_ACCEPT_HEADER,
    [CLIENT_HEADER]: "cli",
    ...(options.cwd ? { [CWD_HEADER]: encodeCwdHeader(options.cwd) } : {}),
  };
  const signal = options.timeoutMs !== undefined ? AbortSignal.timeout(options.timeoutMs) : undefined;

  let sessionId: string | undefined;
  try {
    let initResponse: Response;
    try {
      initResponse = await fetch(mcpUrl, {
        method: "POST",
        headers: mcpHeaders,
        body: JSON.stringify({
          jsonrpc: "2.0",
          method: "initialize",
          params: {
            protocolVersion: MCP_PROTOCOL_VERSION,
            capabilities: {},
            clientInfo: CLIENT_INFO,
          },
          id: INITIALIZE_ID,
        }),
        ...(signal ? { signal } : {}),
      });
    } catch (error) {
      return {
        ok: false,
        mcpUrl,
        error: `failed to reach listener at ${mcpUrl}: ${error instanceof Error ? error.message : String(error)}`,
      };
    }

    const initBody = await parseHttpBody(initResponse, INITIALIZE_ID);
    sessionId = initResponse.headers.get("mcp-session-id") ?? undefined;
    const initError = jsonRpcErrorMessage(initBody);

    if (!initResponse.ok) {
      return {
        ok: false,
        mcpUrl,
        httpStatus: initResponse.status,
        error: `listener initialize returned HTTP ${initResponse.status}`,
      };
    }
    if (initError) {
      return {
        ok: false,
        mcpUrl,
        httpStatus: initResponse.status,
        error: `listener initialize failed: ${initError}`,
      };
    }
    if (!sessionId) {
      return {
        ok: false,
        mcpUrl,
        httpStatus: initResponse.status,
        error: "listener initialize did not return an mcp-session-id",
      };
    }

    let callResponse: Response;
    try {
      callResponse = await fetch(mcpUrl, {
        method: "POST",
        headers: { ...mcpHeaders, "mcp-session-id": sessionId },
        body: JSON.stringify({
          jsonrpc: "2.0",
          method,
          params,
          id: REQUEST_ID,
        }),
        ...(signal ? { signal } : {}),
      });
    } catch (error) {
      return {
        ok: false,
        mcpUrl,
        sessionId,
        error: `${method} request failed: ${error instanceof Error ? error.message : String(error)}`,
      };
    }

    const callBody = await parseHttpBody(callResponse, REQUEST_ID);
    const callError = jsonRpcErrorMessage(callBody);

    if (!callResponse.ok) {
      return {
        ok: false,
        mcpUrl,
        sessionId,
        httpStatus: callResponse.status,
        error: `${method} returned HTTP ${callResponse.status}`,
      };
    }
    if (callError) {
      return { ok: false, mcpUrl, sessionId, httpStatus: callResponse.status, error: callError };
    }

    const result = isRecord(callBody) && "result" in callBody ? callBody.result : undefined;
    if (result === undefined) {
      return {
        ok: false,
        mcpUrl,
        sessionId,
        httpStatus: callResponse.status,
        error: `${method} response did not include a result`,
      };
    }

    return { ok: true, mcpUrl, sessionId, httpStatus: callResponse.status, result };
  } finally {
    if (sessionId) {
      try {
        await fetch(mcpUrl, {
          method: "DELETE",
          headers: { ...mcpHeaders, "mcp-session-id": sessionId },
          ...(signal ? { signal } : {}),
        });
      } catch {
        // best-effort session cleanup; a leaked session times out on its own.
      }
    }
  }
}

/**
 * Thin authenticated HTTP forwarder to a running callmux listener's
 * `tools/call` endpoint. Reused by the `callmux call` CLI verb and later
 * MCP2CLI phases.
 */
export async function callListenerTool(
  url: string,
  toolName: string,
  args: Record<string, unknown> | undefined,
  options: ListenerCallOptions = {}
): Promise<ListenerCallOutcome> {
  const outcome = await listenerRequest(
    url,
    "tools/call",
    { name: toolName, arguments: args ?? {} },
    options
  );

  if (!outcome.ok) {
    return {
      ok: false,
      mcpUrl: outcome.mcpUrl,
      sessionId: outcome.sessionId,
      httpStatus: outcome.httpStatus,
      error: outcome.error,
    };
  }

  const result = isRecord(outcome.result) ? (outcome.result as unknown as CallToolResult) : undefined;
  if (!result) {
    return {
      ok: false,
      mcpUrl: outcome.mcpUrl,
      sessionId: outcome.sessionId,
      httpStatus: outcome.httpStatus,
      error: "tools/call response did not include a result",
    };
  }

  return { ok: true, mcpUrl: outcome.mcpUrl, sessionId: outcome.sessionId, httpStatus: outcome.httpStatus, result };
}

/** Prefer structuredContent, falling back to parsed/raw text content. */
export function extractListenerToolPayload(result: CallToolResult): unknown {
  if (result.structuredContent !== undefined) return result.structuredContent;
  const content = result.content;
  if (!Array.isArray(content)) return undefined;
  const text = content
    .filter((item) => item.type === "text")
    .map((item) => (item as { type: "text"; text: string }).text)
    .join("\n");
  if (text.length === 0) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
