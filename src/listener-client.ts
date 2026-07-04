import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import { listenerUrls, parseHttpBody } from "./doctor.js";

const MCP_ACCEPT_HEADER = "application/json, text/event-stream";
const MCP_PROTOCOL_VERSION = "2025-11-25";
const CWD_HEADER = "x-callmux-cwd";
const CLIENT_INFO = { name: "callmux-cli", version: "1.0" };

export interface ListenerCallOptions {
  cwd?: string;
  headers?: Record<string, string>;
  timeoutMs?: number;
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
 * Thin authenticated HTTP forwarder to a running callmux listener's
 * `tools/call` endpoint. Opens a short-lived MCP session (initialize +
 * tools/call) over plain fetch — no persistent SDK client/transport.
 * Reused by the `callmux call` CLI verb and later MCP2CLI phases.
 */
export async function callListenerTool(
  url: string,
  toolName: string,
  args: Record<string, unknown> | undefined,
  options: ListenerCallOptions = {}
): Promise<ListenerCallOutcome> {
  const { mcpUrl } = listenerUrls(url);
  const baseHeaders = options.headers ?? {};
  const mcpHeaders: Record<string, string> = {
    ...baseHeaders,
    "Content-Type": "application/json",
    Accept: MCP_ACCEPT_HEADER,
    ...(options.cwd ? { [CWD_HEADER]: options.cwd } : {}),
  };
  const signal = options.timeoutMs !== undefined ? AbortSignal.timeout(options.timeoutMs) : undefined;

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
        id: 1,
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

  const initBody = await parseHttpBody(initResponse);
  const sessionId = initResponse.headers.get("mcp-session-id") ?? undefined;
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
        method: "tools/call",
        params: { name: toolName, arguments: args ?? {} },
        id: 2,
      }),
      ...(signal ? { signal } : {}),
    });
  } catch (error) {
    return {
      ok: false,
      mcpUrl,
      sessionId,
      error: `tools/call request failed: ${error instanceof Error ? error.message : String(error)}`,
    };
  }

  const callBody = await parseHttpBody(callResponse);
  const callError = jsonRpcErrorMessage(callBody);

  if (!callResponse.ok) {
    return {
      ok: false,
      mcpUrl,
      sessionId,
      httpStatus: callResponse.status,
      error: `tools/call returned HTTP ${callResponse.status}`,
    };
  }
  if (callError) {
    return { ok: false, mcpUrl, sessionId, httpStatus: callResponse.status, error: callError };
  }

  const result = isRecord(callBody) && isRecord(callBody.result)
    ? (callBody.result as unknown as CallToolResult)
    : undefined;
  if (!result) {
    return {
      ok: false,
      mcpUrl,
      sessionId,
      httpStatus: callResponse.status,
      error: "tools/call response did not include a result",
    };
  }

  return { ok: true, mcpUrl, sessionId, httpStatus: callResponse.status, result };
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
