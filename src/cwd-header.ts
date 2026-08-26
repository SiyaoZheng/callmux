const ENCODED_CWD_PREFIX = "utf8b64:";
const HTTP_VISIBLE_ASCII = /^[\x20-\x7e]+$/;
const BASE64URL = /^[A-Za-z0-9_-]+$/;

/** Encode non-ASCII or control-bearing paths before placing them in an HTTP header. */
export function encodeCwdHeader(cwd: string): string {
  if (HTTP_VISIBLE_ASCII.test(cwd)) return cwd;
  return `${ENCODED_CWD_PREFIX}${Buffer.from(cwd, "utf8").toString("base64url")}`;
}

/** Decode values produced by encodeCwdHeader while preserving legacy plain-ASCII headers. */
export function decodeCwdHeader(value: string | undefined): string | undefined {
  if (value === undefined || !value.startsWith(ENCODED_CWD_PREFIX)) return value;

  const encoded = value.slice(ENCODED_CWD_PREFIX.length);
  if (!encoded || !BASE64URL.test(encoded)) return undefined;

  const decoded = Buffer.from(encoded, "base64url").toString("utf8");
  const canonical = Buffer.from(decoded, "utf8").toString("base64url");
  return canonical === encoded ? decoded : undefined;
}
