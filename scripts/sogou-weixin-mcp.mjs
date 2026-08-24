#!/usr/bin/env node

import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const execFileAsync = promisify(execFile);
const binary = process.env.SOGOU_WEIXIN_BIN ?? "sogou-weixin";

async function callCli(command, args) {
  const cacheDir = await mkdtemp(join(tmpdir(), "sogou-weixin-mcp-"));
  try {
    const { stdout } = await execFileAsync(
      binary,
      ["--json", command, ...args, "--no-cache"],
      {
        env: {
          ...process.env,
          SOGOU_WEIXIN_CACHE: join(cacheDir, "disabled-cache.sqlite3"),
        },
        encoding: "utf8",
        maxBuffer: 16 * 1024 * 1024,
        timeout: 180_000,
      }
    );
    const text = stdout.trim();
    const payload = JSON.parse(text);
    return {
      content: [{ type: "text", text }],
      structuredContent: payload,
      ...(payload.ok === false ? { isError: true } : {}),
    };
  } catch (error) {
    const stdout = typeof error?.stdout === "string" ? error.stdout.trim() : "";
    if (stdout) {
      try {
        const payload = JSON.parse(stdout);
        return {
          content: [{ type: "text", text: stdout }],
          structuredContent: payload,
          isError: true,
        };
      } catch {}
    }
    return {
      content: [{ type: "text", text: error instanceof Error ? error.message : String(error) }],
      isError: true,
    };
  } finally {
    await rm(cacheDir, { recursive: true, force: true });
  }
}

const server = new McpServer({ name: "sogou-weixin", version: "1.0.0" });
const annotations = { readOnlyHint: true, idempotentHint: true };

server.registerTool(
  "sogou_weixin_search",
  {
    description: "Search public WeChat articles through the installed sogou-weixin CLI.",
    annotations,
    inputSchema: {
      query: z.string().min(1),
      page: z.number().int().min(1).optional(),
      limit: z.number().int().min(1).max(10).optional(),
    },
  },
  async ({ query, page, limit }) => callCli("search", [
    query,
    ...(page === undefined ? [] : ["--page", String(page)]),
    ...(limit === undefined ? [] : ["--limit", String(limit)]),
  ])
);

server.registerTool(
  "sogou_weixin_fetch",
  {
    description: "Fetch one public WeChat article using a sogou-weixin fetch_ref or public URL.",
    annotations,
    inputSchema: {
      fetch_ref: z.string().min(1),
      format: z.enum(["text", "html", "both"]).optional(),
      max_chars: z.number().int().min(1).max(2_000_000).optional(),
    },
  },
  async ({ fetch_ref, format, max_chars }) => callCli("fetch", [
    fetch_ref,
    ...(format === undefined ? [] : ["--format", format]),
    ...(max_chars === undefined ? [] : ["--max-chars", String(max_chars)]),
  ])
);

process.on("SIGTERM", async () => {
  await server.close();
  process.exit(0);
});

await server.connect(new StdioServerTransport());
