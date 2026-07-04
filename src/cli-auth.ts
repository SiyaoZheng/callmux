import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { getDefaultConfigPath } from "./config.js";

/**
 * Client -> callmux authentication for the CLI.
 *
 * This module ONLY resolves and presents a bearer token on the client side.
 * It deliberately does not authenticate or authorize anything itself: the
 * daemon keeps ownership of that via the existing `authenticateBearerToken`
 * (src/auth.ts), the OIDC verifier (src/oidc.ts), and `evaluateToolAuthorization`
 * (src/authorization.ts). The CLI's only job is to find the right token and put
 * it in an `Authorization: Bearer <token>` header.
 */

const CALLMUX_TOKEN_ENV = "CALLMUX_TOKEN";
const MANAGED_TOKEN_FILENAME = "cli-token";

export interface ClientTokenSources {
  /** `--token <t>` inline value (tier 3). */
  token?: string;
  /** `--token-file <path>` (tier 3, keeps the secret out of `ps`/shell history). */
  tokenFile?: string;
  /** Overrides `process.env.CALLMUX_TOKEN` (tier 2). Mainly for tests. */
  env?: string;
  /** Overrides the managed CLI token store path (tier 4). Mainly for tests. */
  managedTokenPath?: string;
}

/**
 * Path of the managed CLI token store that `callmux client attach --token`
 * writes and `resolveClientToken` reads as its lowest-priority source. Lives
 * next to the daemon config so one `attach` can wire both the MCP client and
 * the CLI.
 */
export function getManagedClientTokenPath(): string {
  return join(dirname(getDefaultConfigPath()), MANAGED_TOKEN_FILENAME);
}

function cleanToken(raw: string | undefined): string | undefined {
  if (raw === undefined) return undefined;
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

async function readTokenFile(path: string): Promise<string | undefined> {
  try {
    return cleanToken(await readFile(path, "utf-8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw error;
  }
}

/**
 * Resolve the client -> callmux bearer token using the documented precedence
 * (cheapest-trust-first). Returns `undefined` when no token is found, which is
 * the correct result for a loopback daemon: the listener already permits
 * loopback without auth (src/listener.ts), so we send no `Authorization` header
 * and let the daemon decide.
 *
 * Order:
 *   1. Loopback daemon -> no token required (this function returning
 *      `undefined` is that case; the caller simply omits the header).
 *   2. `CALLMUX_TOKEN` env var.
 *   3. `--token` / `--token-file` (`--token` wins if both are given).
 *   4. The managed CLI token store written by `callmux client attach`.
 */
export async function resolveClientToken(
  sources: ClientTokenSources = {}
): Promise<string | undefined> {
  // 2. CALLMUX_TOKEN env var.
  const envToken = cleanToken(
    sources.env !== undefined ? sources.env : process.env[CALLMUX_TOKEN_ENV]
  );
  if (envToken) return envToken;

  // 3. --token / --token-file.
  const inlineToken = cleanToken(sources.token);
  if (inlineToken) return inlineToken;
  if (sources.tokenFile) {
    const fileToken = await readTokenFile(resolve(sources.tokenFile));
    if (fileToken) return fileToken;
    throw new Error(
      `--token-file "${sources.tokenFile}" did not contain a non-empty token`
    );
  }

  // 4. Managed CLI token store from `callmux client attach`.
  const managedPath = sources.managedTokenPath ?? getManagedClientTokenPath();
  return await readTokenFile(managedPath);
}

/** True when `headers` already carries an Authorization header (any casing). */
function hasAuthorizationHeader(
  headers: Record<string, string>
): boolean {
  return Object.keys(headers).some(
    (key) => key.toLowerCase() === "authorization"
  );
}

/**
 * Return `headers` with `Authorization: Bearer <token>` added, unless the token
 * is empty or the caller already set an Authorization header (an explicit
 * `--header Authorization:...` always wins).
 */
export function withBearerToken(
  headers: Record<string, string>,
  token: string | undefined
): Record<string, string> {
  if (!token || hasAuthorizationHeader(headers)) return headers;
  return { ...headers, Authorization: `Bearer ${token}` };
}

/**
 * Persist `token` to the managed CLI token store (0600) so the CLI and any
 * `callmux bridge` subprocess can resolve it later. Returns the path written.
 */
export async function writeManagedClientToken(
  token: string,
  managedTokenPath?: string
): Promise<string> {
  const cleaned = cleanToken(token);
  if (!cleaned) {
    throw new Error("cannot stash an empty CLI token");
  }
  const path = managedTokenPath ?? getManagedClientTokenPath();
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${cleaned}\n`, { encoding: "utf-8", mode: 0o600 });
  return path;
}
