import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
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
  /**
   * Skip the managed-store fallback (tier 4). Used by `client attach` when
   * resolving what to (re)stash: reading the store we're about to write would
   * make an attach with no new token flags silently recycle whatever is
   * already there.
   */
  skipManagedStore?: boolean;
}

/** Which precedence tier actually supplied the resolved token, for error messages. */
export type ClientTokenSourceTier = "env" | "flag" | "token-file" | "managed-store" | "none";

export interface ResolvedClientToken {
  token?: string;
  source: ClientTokenSourceTier;
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
 * (cheapest-trust-first), and report which tier supplied it (or "none") so
 * callers can name it in error messages — e.g. a stale `CALLMUX_TOKEN` env var
 * beating a correct `--token` flag would otherwise surface as a bare "HTTP
 * 401" with no clue which token was sent. An absent token is the correct
 * result for a loopback daemon: the listener already permits loopback without
 * auth (src/listener.ts), so we send no `Authorization` header and let the
 * daemon decide.
 *
 * Order:
 *   1. Loopback daemon -> no token required ("none" is that case; the caller
 *      simply omits the header).
 *   2. `CALLMUX_TOKEN` env var.
 *   3. `--token` / `--token-file` (`--token` wins if both are given).
 *   4. The managed CLI token store written by `callmux client attach`.
 */
export async function resolveClientTokenDetailed(
  sources: ClientTokenSources = {}
): Promise<ResolvedClientToken> {
  // 2. CALLMUX_TOKEN env var.
  const envToken = cleanToken(
    sources.env !== undefined ? sources.env : process.env[CALLMUX_TOKEN_ENV]
  );
  if (envToken) return { token: envToken, source: "env" };

  // 3. --token / --token-file.
  const inlineToken = cleanToken(sources.token);
  if (inlineToken) return { token: inlineToken, source: "flag" };
  if (sources.tokenFile) {
    const fileToken = await readTokenFile(resolve(sources.tokenFile));
    if (fileToken) return { token: fileToken, source: "token-file" };
    throw new Error(
      `--token-file "${sources.tokenFile}" did not contain a non-empty token`
    );
  }

  // 4. Managed CLI token store from `callmux client attach`.
  if (sources.skipManagedStore) return { source: "none" };
  const managedPath = sources.managedTokenPath ?? getManagedClientTokenPath();
  const managedToken = await readTokenFile(managedPath);
  return managedToken ? { token: managedToken, source: "managed-store" } : { source: "none" };
}

export async function resolveClientToken(
  sources: ClientTokenSources = {}
): Promise<string | undefined> {
  return (await resolveClientTokenDetailed(sources)).token;
}

/** Human-readable label for the tier a resolved token came from, for 401 error messages. */
export function describeClientTokenSource(
  resolved: ResolvedClientToken,
  sources: ClientTokenSources = {}
): string {
  switch (resolved.source) {
    case "env":
      return `sent the token from the ${CALLMUX_TOKEN_ENV} env var`;
    case "flag":
      return "sent the token from --token";
    case "token-file":
      return `sent the token from --token-file "${sources.tokenFile ?? ""}"`;
    case "managed-store":
      return `sent the token from the managed store (${sources.managedTokenPath ?? getManagedClientTokenPath()})`;
    case "none":
      return "sent no token (none configured)";
  }
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
  // `mode` on writeFile only applies when the file is newly created; a
  // pre-existing file with looser permissions keeps them unless we chmod it.
  await chmod(path, 0o600);
  return path;
}
