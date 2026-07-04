[< Back to README](../README.md)

# MCP2CLI: Turn Any MCP Tool Into a Shell Call

Every MCP tool your agent is connected to costs context tokens just by existing. Its name, description, and full input schema sit in the tool list on **every single turn**, whether you call it once a session or never. Connect 40 tools across 5 servers and you're re-paying for all 40 schemas on every API round-trip, even the ones you use once and never touch again.

MCP2CLI is callmux's answer: every tool it proxies is also reachable as a shell command. `callmux call <tool> '<args>'` runs the exact same `tools/call` path an MCP client would use, but from Bash — the one tool your agent already has, reaching unlimited downstream capability. Schemas load on demand via `tools schema <tool>`, only for the tool you're about to call, not for the other 39 riding along for free.

This page is the conceptual case for when and why to reach for the CLI. For the exhaustive command/flag tables, see the [CLI Reference](cli-reference.md).

---

## The core argument: pay for schemas, not for tool lists

An MCP connection is a standing cost. The moment a server is attached, its tools' full JSON schemas — types, enums, descriptions, defaults, bounds — are serialized into the system prompt on every turn. That cost doesn't scale with how often you use a tool. A `search_issues` you call fifty times a session and a `delete_deployment` you call once both pay the identical per-turn tax.

The CLI inverts that. Discovery is nearly free:

```bash
callmux tools list
callmux tools search issue
```

`tools list`/`tools search` hit the daemon's `tools/list` once and print only names plus one-line descriptions — enough for an agent to decide what's callable, at a tiny fraction of a full schema dump. You only pay for the full input schema when you're actually about to use a tool:

```bash
callmux tools schema github__create_issue
```

That schema is fetched, read, used, and gone — it doesn't sit in context for the rest of the session the way a connected tool definition does. For a tool you'll call once, that's the entire cost difference: one schema fetch instead of N turns of a standing schema tax.

The result: a CLI is functionally one tool (Bash) with access to everything callmux proxies, at a cost that scales with actual usage instead of with how many servers you've connected.

---

## The moat: secrets never reach the agent

This isn't just a context-budget trick — it changes where secrets live.

Compare it to installing `gh` directly: the GitHub token has to be somewhere the agent's shell can read it, typically `GITHUB_TOKEN` in the environment or `gh`'s own config file. Any tool call the agent makes, any subprocess it spawns, any prompt injection that gets it to `env` or `cat ~/.config/gh/hosts.yml` — the token is right there.

`callmux call` flips that. The agent's shell only ever holds a **client→callmux bearer token** (or nothing at all, on loopback). Downstream credentials — `GITHUB_TOKEN`, database URLs, API keys for whatever the daemon proxies — live exclusively in the daemon's config, server-side. The agent names a tool (`github__create_issue`) and passes arguments; it never sees, never needs, and structurally *cannot* hold the credential that tool executes with.

That's a real security boundary, not a convention: even a fully compromised agent shell only leaks a bearer token scoped to "call tools I'm authorized for," not the downstream secrets those tools use.

---

## Complement, not replacement

MCP2CLI doesn't mean drop the MCP connection. It means stop paying full price for tools you barely use.

**Keep on the MCP connection (hot tools):**
- Tools called repeatedly within a session
- Anything where you want structured `structuredContent` results without re-parsing CLI stdout
- Tools where per-call authorization errors should surface as part of the normal tool-call flow
- Anything latency-sensitive — the CLI opens and tears down an MCP session per invocation; a live connection doesn't

**Push behind `callmux call` (long-tail tools):**
- Tools you expect to call once or rarely this session
- Rarely-used servers you keep configured "just in case" — their schemas shouldn't tax every turn for a capability you touch once a week
- One-off diagnostic or admin calls you don't want cluttering the always-on tool list at all

Use both in the same session. A coding agent might keep its primary `github` server connected for the dozen `search_issues`/`get_file_contents` calls it makes every session, while reaching for `callmux call slack__post_message` via CLI the one time it needs to ping a channel. Nothing about the CLI path requires disconnecting anything.

---

## A worked verb tour

### `call` — one tool, one invocation

```bash
callmux call github__search_issues '{"query":"is:open"}'
callmux call github__create_issue --file payload.json
```

Arguments are JSON. For anything beyond a one-liner, `--file <path>` reads the argument object from disk instead of fighting shell quoting — the escape hatch for nested objects, arrays, or multi-line string fields.

Exit codes: `0` success, `1` the downstream tool itself reported an error (`isError: true` — a real tool-level failure, like a 404 from the API it wraps), `2` a usage or transport failure (bad flag, invalid JSON, listener unreachable). The distinction matters for scripting: retry logic for `2` (transport hiccup) looks different from retry logic for `1` (the call was fine, the tool said no).

### `tools list` / `tools schema` / `tools search` — pay-as-you-go discovery

```bash
callmux tools list --server github
callmux tools schema github__create_issue
callmux tools search issue
```

`list` and `search` are the cheap path — names and one-liners, no schemas. `schema` is the only command that pulls a full input schema, and it pulls exactly one. This is the whole trick in three commands: browse cheaply, pay precisely.

### `parallel` / `batch` / `pipeline` — the sugar verbs, from the shell

The same fan-out primitives available as MCP meta-tools (`callmux_parallel`, `callmux_batch`, `callmux_pipeline`) are CLI verbs too:

```bash
callmux parallel 'github__issue_read {"number":1}' 'github__issue_read {"number":2}'
callmux batch github__issue_read '[{"number":1},{"number":2},{"number":3}]'
callmux pipeline 'github__search_issues {"query":"is:open"}' 'github__issue_read {"$json":"[0].number"}'
```

Each `parallel` argument is `<tool> <argsJSON>` — one shell token for the tool name, split by the first space from the JSON that follows. That argv-vs-JSON split is exact: everything before the first space is the tool name, everything after is parsed as JSON. When a step's arguments are too large or too structured to fit that split cleanly (nested objects, nested arrays, nested pipelines), drop to `--file`:

```bash
callmux pipeline --file plan.json
```

`plan.json` holds the full step list as structured JSON — the same content you'd otherwise fight to fit into one argv token. Same exit codes as `call`: `0`, `1` (a step reported a tool-level error), `2` (usage/transport failure, or execution halted before completion).

---

## Auth: bearer tokens for callmux, never for downstream

### Token precedence

A loopback daemon (`127.0.0.1`/`::1`/`localhost`) needs no token at all — auth is only enforced for remote or explicitly auth-configured listeners. When one is required, the CLI resolves it in this order, cheapest-trust-first:

1. **Loopback** → no token required.
2. **`CALLMUX_TOKEN`** environment variable.
3. **`--token <t>`** / **`--token-file <path>`** (`--token` wins if both given; `--token-file` keeps the secret out of `ps` output and shell history — prefer it over `--token` for anything scripted or logged).
4. The **managed CLI token store** (`~/.config/callmux/cli-token`), written by `callmux client attach --token ... --yes`.

```bash
# Local dev, loopback daemon: nothing to configure.
callmux call github__search_issues '{"query":"is:open"}'

# Remote daemon, token from a file instead of argv:
callmux call github__search_issues '{"query":"is:open"}' \
  --url https://mux.example.com/mcp --token-file ~/.secrets/callmux.token
```

This bearer token authenticates the agent **to callmux**. It is never the downstream credential (`GITHUB_TOKEN` and friends) — those stay in the daemon's config regardless of which tier resolved the CLI's token.

### Per-tool authorization is enforced server-side

The harness or CI system granting your agent shell access typically only grants something coarse — `callmux call:*`, "this agent may run the callmux CLI." That's fine, because the fine-grained decision happens on the daemon, not in what the harness hands out.

Give the agent a single broad-looking token, then let callmux's authorization policy narrow it. The **deny-write principal pattern**: issue a read-ish principal by denying the mutating tool shapes, regardless of what the harness's grant looks like from the outside.

```jsonc
// callmux config.json (daemon side)
{
  "auth": {
    "mode": "bearer",
    "tokens": [{ "id": "agent-readonly", "hash": "scrypt$..." }]
  },
  "authorization": {
    "defaultEffect": "allow",
    "rules": [
      { "id": "deny-writes", "effect": "deny", "principals": ["*"], "tools": ["*__*write*"] }
    ]
  }
}
```

With the `agent-readonly` token, `callmux call github__search_issues ...` succeeds and `callmux call github__create_issue ...` comes back as an `authorization_denied` result (exit `1`) — a normal tool-level error, not a transport failure. The agent's harness grant said "call anything"; the daemon actually enforced "read only." Flip `defaultEffect` to `deny` and allow-list exact read tools for a fail-closed policy instead. Full schema in the [Config Reference](config-reference.md).

### One audit trail, not two

CLI and MCP calls hit the same authentication, the same authorization policy, and the same SQLite event store — tagged with `transport: cli` or `transport: mcp` so `/dashboard/drilldown` can split them apart. Nothing about switching a tool from "connected via MCP" to "called via CLI" creates a second logging path or a second policy to maintain.

---

## See also

- [CLI Reference](cli-reference.md) — full command/flag tables, exit codes, environment variables
- [Meta-Only Mode](meta-only-mode.md) — the MCP-side equivalent: fixed system prompt size via `callmux_call`/`callmux_search_tools` meta-tools instead of shell verbs
- [Observability](observability.md) — the shared audit trail behind `/dashboard/drilldown`'s `byTransport` breakdown
