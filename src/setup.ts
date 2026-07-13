import * as p from "@clack/prompts";
import { UpstreamManager } from "./upstream.js";
import {
  getDefaultConfigPath,
  loadManagedConfig,
  saveManagedConfig,
  saveManagedSecret,
} from "./config.js";
import { parseCommandLine } from "./cli.js";
import {
  attachClaudeConfig,
  attachCodexConfig,
  getDefaultClientConfigPath,
  type ClientKind,
} from "./client-config.js";
import {
  createDaemonPlan,
  detectDaemonEnvironment,
  executeDaemonPlan,
  type DaemonScope,
} from "./daemon.js";
import { detectExistingConfigs, type DetectedServer } from "./detect.js";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { SERVER_REGISTRY, type RegistryEntry } from "./registry.js";
import { isHttpServerConfig } from "./types.js";
import { META_TOOLS } from "./meta-tools.js";
import type { CallmuxConfig, ServerConfig } from "./types.js";

interface DiscoveredServer {
  name: string;
  config: ServerConfig;
  tools: string[];
  selectedTools?: string[];
  pendingSecrets?: Array<{ path: string; value: string }>;
}

type SetupClientMode =
  | { mode: "local" }
  | { mode: "shared"; listenerUrl: string };

export function setupPromptDefaults(existing?: CallmuxConfig | null): {
  cacheEnabled: boolean;
  cacheTtlWhenEnabled: number;
  metaOnly: boolean;
  descriptionMaxLength?: number;
} {
  const existingCacheTtl = existing?.cacheTtlSeconds ?? 0;
  return {
    cacheEnabled: existing ? existingCacheTtl > 0 : true,
    cacheTtlWhenEnabled: existingCacheTtl > 0 ? existingCacheTtl : 60,
    metaOnly: existing?.metaOnly ?? false,
    ...(existing?.descriptionMaxLength !== undefined
      ? { descriptionMaxLength: existing.descriptionMaxLength }
      : {}),
  };
}

export function listenerClientUrl(baseUrl: string, client: ClientKind): string {
  const url = new URL(baseUrl);
  url.pathname = client === "codex" ? "/mcp" : "/sse";
  url.search = "";
  url.hash = "";
  return url.href;
}

export function renderSharedListenerStartCommand(
  listenerUrl: string,
  configPath: string
): string {
  const url = new URL(listenerUrl);
  const port = url.port || (url.protocol === "https:" ? "443" : "80");
  const host = url.hostname;
  const args = ["callmux", "--listen", port, "--config", configPath];
  if (host !== "localhost" && host !== "127.0.0.1" && host !== "::1") {
    args.splice(3, 0, "--host", host);
  }
  return args.join(" ");
}

export async function runSetup(configPath?: string): Promise<void> {
  const resolvedConfigPath = configPath ?? getDefaultConfigPath();

  p.intro("callmux setup");

  const existing = await loadManagedConfig(resolvedConfigPath);
  let effectiveExisting = existing;
  if (existing && Object.keys(existing.servers).length > 0) {
    const action = await p.select({
      message: `Found existing config at ${resolvedConfigPath} with ${Object.keys(existing.servers).length} server(s). What would you like to do?`,
      options: [
        { value: "extend", label: "Add more servers to existing config" },
        { value: "replace", label: "Start fresh (overwrites current config)" },
        { value: "cancel", label: "Cancel" },
      ],
    });

    if (p.isCancel(action) || action === "cancel") {
      p.cancel("Setup cancelled.");
      process.exit(0);
    }

    if (action === "replace") {
      effectiveExisting = null;
    }
  }

  const imported = await detectAndImport();
  const servers = await selectServers();

  if (imported.length === 0 && servers.length === 0) {
    p.cancel("No servers selected.");
    process.exit(0);
  }

  const discovered = await discoverTools(servers, imported, resolvedConfigPath);

  const promptDefaults = setupPromptDefaults(effectiveExisting);
  const cacheChoice = await p.confirm({
    message: "Enable caching for read-only tools? (recommended)",
    initialValue: promptDefaults.cacheEnabled,
  });

  if (p.isCancel(cacheChoice)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  const cacheTtl = cacheChoice ? promptDefaults.cacheTtlWhenEnabled : 0;

  const totalToolCount = discovered.reduce(
    (sum, s) => sum + (s.selectedTools?.length ?? s.tools.length),
    0
  );

  const metaOnlyChoice = await p.confirm({
    message: `Enable meta-only mode? Hides individual tools from your agent's listing and exposes them only through callmux meta-tools. Reduces tool listing from ${totalToolCount} tools to ${META_TOOLS.length}.`,
    initialValue: promptDefaults.metaOnly,
  });

  if (p.isCancel(metaOnlyChoice)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  let descriptionMaxLength: number | undefined;
  if (metaOnlyChoice) {
    const descMaxInput = await p.text({
      message: "Max description length for tool discovery (leave blank for no limit):",
      placeholder: "100",
      initialValue: promptDefaults.descriptionMaxLength?.toString() ?? "",
      validate: (v = "") => {
        if (v && (!/^\d+$/.test(v) || Number(v) < 1))
          return "Must be a positive integer";
      },
    });

    if (p.isCancel(descMaxInput)) {
      p.cancel("Setup cancelled.");
      process.exit(0);
    }

    descriptionMaxLength = descMaxInput ? Number(descMaxInput) : undefined;
  }

  const config = buildSetupConfig(
    discovered,
    cacheTtl,
    metaOnlyChoice,
    descriptionMaxLength,
    effectiveExisting
  );

  const clientMode = await selectClientMode();

  for (const pending of discovered.flatMap((server) => server.pendingSecrets ?? [])) {
    await saveManagedSecret(pending.path, pending.value);
  }
  await saveManagedConfig(resolvedConfigPath, config);
  p.log.success(`Config written to ${resolvedConfigPath}`);

  if (clientMode.mode === "shared") {
    p.log.info(
      `Start the shared listener with: ${renderSharedListenerStartCommand(clientMode.listenerUrl, resolvedConfigPath)}`
    );
    await offerDaemonInstall(resolvedConfigPath, clientMode.listenerUrl);
  }

  await attachToClients(resolvedConfigPath, clientMode);

  p.outro(
    clientMode.mode === "shared"
      ? "Setup complete. Start the listener, then clients will connect to the shared callmux URL."
      : "Setup complete. Your agent now has access to callmux meta-tools."
  );
}

async function offerDaemonInstall(
  configPath: string,
  listenerUrl: string
): Promise<void> {
  const installDaemon = await p.confirm({
    message: "Install callmux as a background daemon for this shared listener?",
    initialValue: true,
  });

  if (p.isCancel(installDaemon) || !installDaemon) {
    return;
  }

  const scopeChoice = await p.select({
    message: "Daemon scope:",
    options: [
      { value: "user", label: "User service", hint: "Recommended; no sudo required" },
      { value: "system", label: "System service", hint: "Requires admin/root permissions" },
    ],
  });

  if (p.isCancel(scopeChoice)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  const startNow = await p.confirm({
    message: "Start the daemon now?",
    initialValue: true,
  });
  if (p.isCancel(startNow)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  const enableAtLogin = await p.confirm({
    message: "Enable the daemon at login/boot?",
    initialValue: true,
  });
  if (p.isCancel(enableAtLogin)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  const url = new URL(listenerUrl);
  const port = Number(url.port || (url.protocol === "https:" ? "443" : "80"));
  const host = url.hostname;
  const env = await detectDaemonEnvironment();
  const plan = createDaemonPlan(
    {
      action: "install",
      configPath,
      port,
      ...(host !== "localhost" && host !== "127.0.0.1" && host !== "::1" ? { host } : {}),
      scope: scopeChoice as DaemonScope,
      start: startNow === true,
      enable: enableAtLogin === true,
    },
    env
  );

  try {
    const result = await executeDaemonPlan(plan);
    p.log.success(`Daemon installed (${plan.kind}, ${plan.scope}).`);
    if (result.output.trim()) {
      p.log.info(result.output);
    }
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    p.log.error(`Daemon install failed: ${msg}`);
    p.log.info("You can retry with `callmux daemon install --start --enable`.");
  }
}

async function selectClientMode(): Promise<SetupClientMode> {
  const mode = await p.select({
    message: "How should clients connect to callmux?",
    options: [
      {
        value: "shared",
        label: "Shared listener URL",
        hint: "Run one callmux --listen process and connect all sessions to it",
      },
      {
        value: "local",
        label: "Local command per client",
        hint: "Each client starts its own callmux process",
      },
    ],
  });

  if (p.isCancel(mode)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  if (mode === "local") {
    return { mode: "local" };
  }

  const listenerUrl = await p.text({
    message: "Shared listener base URL:",
    placeholder: "http://localhost:4860",
    initialValue: "http://localhost:4860",
    validate: (value = "") => {
      try {
        new URL(value);
      } catch {
        return "Must be a valid URL";
      }
    },
  });

  if (p.isCancel(listenerUrl)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  return { mode: "shared", listenerUrl };
}

async function detectAndImport(): Promise<DiscoveredServer[]> {
  const detection = await detectExistingConfigs();

  if (detection.servers.length === 0) return [];

  const grouped = new Map<string, DetectedServer[]>();
  for (const server of detection.servers) {
    const list = grouped.get(server.source) ?? [];
    list.push(server);
    grouped.set(server.source, list);
  }

  p.log.info(
    `Found ${detection.servers.length} existing MCP server(s) in: ${[...grouped.keys()].join(", ")}`
  );

  const options = detection.servers.map((s) => {
    const hint = isHttpServerConfig(s.config) ? s.config.url : `${(s.config as { command: string }).command}`;
    return {
      value: s.name,
      label: `${s.name} (${s.source})`,
      hint,
    };
  });

  const selected = await p.multiselect({
    message: "Import existing servers into callmux?",
    options,
    required: false,
  });

  if (p.isCancel(selected) || selected.length === 0) return [];

  const imported: DiscoveredServer[] = [];
  for (const name of selected) {
    const server = detection.servers.find((s) => s.name === name)!;
    imported.push({ name: server.name, config: server.config, tools: [] });
  }

  return imported;
}

async function selectServers(): Promise<Array<{ entry?: RegistryEntry; custom?: { name: string; command: string; url?: string } }>> {
  const registryOptions = SERVER_REGISTRY.map((entry) => ({
    value: entry.name,
    label: entry.label,
    hint: entry.description,
  }));

  const selected = await p.multiselect({
    message: "Which MCP servers do you want to connect?",
    options: [
      ...registryOptions,
      { value: "__custom__", label: "Custom server", hint: "Enter command manually" },
    ],
    required: true,
  });

  if (p.isCancel(selected)) {
    p.cancel("Setup cancelled.");
    process.exit(0);
  }

  const results: Array<{ entry?: RegistryEntry; custom?: { name: string; command: string; url?: string } }> = [];

  for (const name of selected) {
    if (name === "__custom__") {
      const customName = await p.text({
        message: "Name for your custom server (used as identifier):",
        placeholder: "my-server",
        validate: (v = "") => {
          if (!v.trim()) return "Name is required";
          if (!/^[a-z0-9-]+$/.test(v)) return "Use lowercase letters, numbers, and hyphens only";
        },
      });

      if (p.isCancel(customName)) {
        p.cancel("Setup cancelled.");
        process.exit(0);
      }

      const connectionType = await p.select({
        message: "How does this server connect?",
        options: [
          { value: "stdio", label: "Local command (stdio)", hint: "npx, node, python, etc." },
          { value: "url", label: "Remote URL (HTTP/SSE)", hint: "https://..." },
        ],
      });

      if (p.isCancel(connectionType)) {
        p.cancel("Setup cancelled.");
        process.exit(0);
      }

      if (connectionType === "url") {
        const customUrl = await p.text({
          message: "Server URL:",
          placeholder: "https://mcp.example.com/sse",
          validate: (v = "") => {
            if (!v.trim()) return "URL is required";
            try { new URL(v); } catch { return "Must be a valid URL"; }
          },
        });

        if (p.isCancel(customUrl)) {
          p.cancel("Setup cancelled.");
          process.exit(0);
        }

        results.push({ custom: { name: customName, command: "", url: customUrl } });
      } else {
        const customCommand = await p.text({
          message: "Command to start the server:",
          placeholder: "npx -y @modelcontextprotocol/server-something",
          validate: (v = "") => {
            if (!v.trim()) return "Command is required";
            try {
              parseCommandLine(v);
            } catch (error) {
              return error instanceof Error ? error.message : String(error);
            }
          },
        });

        if (p.isCancel(customCommand)) {
          p.cancel("Setup cancelled.");
          process.exit(0);
        }

        results.push({ custom: { name: customName, command: customCommand } });
      }
    } else {
      results.push({ entry: SERVER_REGISTRY.find((e) => e.name === name)! });
    }
  }

  return results;
}

interface SetupEnvSelection {
  envRefs: Record<string, string>;
  probeEnv: Record<string, string>;
  pendingSecrets: Array<{ path: string; value: string }>;
}

function setupSecretPath(configPath: string, serverName: string, envName: string): string {
  const safeServer = serverName.replace(/[^A-Za-z0-9_-]/g, "_");
  const safeEnvName = envName.replace(/[^A-Za-z0-9_-]/g, "_");
  return join(
    dirname(resolve(configPath)),
    "secrets",
    `${safeServer}-${safeEnvName}`
  );
}

async function promptEnvVars(
  entry: RegistryEntry,
  configPath: string
): Promise<SetupEnvSelection> {
  const envRefs: Record<string, string> = {};
  const probeEnv: Record<string, string> = {};
  const pendingSecrets: Array<{ path: string; value: string }> = [];

  for (const spec of entry.envVars) {
    const detected = process.env[spec.name];
    const source = await p.select({
      message: `How should callmux load ${spec.name}?`,
      options: [
        {
          value: "environment",
          label: `Reference environment variable ${spec.name}`,
          hint: detected
            ? "Detected; recommended"
            : "Set it before starting callmux",
        },
        {
          value: "file",
          label: "Enter and store in a private secret file",
          hint: "Masked input; stored with mode 0600",
        },
        ...(!spec.required
          ? [{ value: "skip", label: "Skip this optional value" }]
          : []),
      ],
    });

    if (p.isCancel(source)) {
      p.cancel("Setup cancelled.");
      process.exit(0);
    }

    if (source === "skip") continue;
    if (source === "environment") {
      envRefs[spec.name] = `env:${spec.name}`;
      if (detected) probeEnv[spec.name] = detected;
      continue;
    }

    const value = await p.password({
      message: `${spec.description}${spec.hint ? ` (${spec.hint})` : ""}:`,
      validate: (v = "") => {
        if (spec.required && !v.trim()) return `${spec.name} is required`;
      },
    });
    if (p.isCancel(value)) {
      p.cancel("Setup cancelled.");
      process.exit(0);
    }
    if (!value.trim()) continue;

    const path = setupSecretPath(configPath, entry.name, spec.name);
    envRefs[spec.name] = `file:${path}`;
    probeEnv[spec.name] = value.trim();
    pendingSecrets.push({ path, value: value.trim() });
  }

  return { envRefs, probeEnv, pendingSecrets };
}

async function discoverTools(
  servers: Array<{ entry?: RegistryEntry; custom?: { name: string; command: string; url?: string } }>,
  preImported: DiscoveredServer[] = [],
  configPath = getDefaultConfigPath()
): Promise<DiscoveredServer[]> {
  const discovered: DiscoveredServer[] = [...preImported];

  for (const server of servers) {
    const name = server.entry?.name ?? server.custom!.name;
    const label = server.entry?.label ?? server.custom!.name;

    let envSelection: SetupEnvSelection = {
      envRefs: {},
      probeEnv: {},
      pendingSecrets: [],
    };
    if (server.entry && server.entry.envVars.length > 0) {
      envSelection = await promptEnvVars(server.entry, configPath);
    }

    let config: ServerConfig;

    if (server.custom?.url) {
      config = { url: server.custom.url };
    } else if (server.entry) {
      config = {
        command: server.entry.command,
        args: [...server.entry.args],
        ...(Object.keys(envSelection.envRefs).length > 0
          ? { envRefs: envSelection.envRefs }
          : {}),
      };
    } else {
      const parts = parseCommandLine(server.custom!.command);
      config = {
        command: parts[0],
        args: parts.slice(1),
      };
    }

    const s = p.spinner();
    s.start(`Connecting to ${label}...`);

    const upstream = new UpstreamManager();
    let tools: string[] = [];

    try {
      const probeConfig: ServerConfig = "command" in config
        ? (() => {
            const { envRefs: _envRefs, ...withoutRefs } = config;
            return {
              ...withoutRefs,
              ...(Object.keys(envSelection.probeEnv).length > 0
                ? { env: { ...(withoutRefs.env ?? {}), ...envSelection.probeEnv } }
                : {}),
            };
          })()
        : config;
      const [connection] = await upstream.connect({ [name]: probeConfig });
      tools = connection?.tools.map((t) => t.name).sort() ?? [];
      s.stop(`${label}: found ${tools.length} tool${tools.length === 1 ? "" : "s"}`);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      s.stop(`${label}: connection failed`);

      const action = await p.select({
        message: `Could not connect to ${label}: ${msg}. What do you want to do?`,
        options: [
          { value: "skip", label: "Skip this server" },
          { value: "add-anyway", label: "Add without tool discovery (expose all tools)" },
        ],
      });

      if (p.isCancel(action) || action === "skip") {
        continue;
      }
    } finally {
      await upstream.close();
    }

    let selectedTools: string[] | undefined;

    if (tools.length > 0) {
      const toolChoice = await p.select({
        message: `${label} exposes ${tools.length} tools. Which do you want?`,
        options: [
          { value: "all", label: `All ${tools.length} tools`, hint: tools.slice(0, 5).join(", ") + (tools.length > 5 ? "..." : "") },
          { value: "pick", label: "Pick individually" },
        ],
      });

      if (p.isCancel(toolChoice)) {
        p.cancel("Setup cancelled.");
        process.exit(0);
      }

      if (toolChoice === "pick") {
        const picked = await p.multiselect({
          message: `Select tools from ${label}:`,
          options: tools.map((t) => ({ value: t, label: t })),
          required: true,
        });

        if (p.isCancel(picked)) {
          p.cancel("Setup cancelled.");
          process.exit(0);
        }

        selectedTools = picked;
      }
    }

    discovered.push({
      name,
      config,
      tools,
      selectedTools,
      ...(envSelection.pendingSecrets.length > 0
        ? { pendingSecrets: envSelection.pendingSecrets }
        : {}),
    });
  }

  return discovered;
}

export function buildSetupConfig(
  discovered: DiscoveredServer[],
  cacheTtl: number,
  metaOnly: boolean,
  descriptionMaxLength: number | undefined,
  existing?: CallmuxConfig | null
): CallmuxConfig {
  const servers: Record<string, ServerConfig> = {
    ...(existing?.servers ?? {}),
  };

  for (const { name, config, selectedTools } of discovered) {
    servers[name] = {
      ...config,
      ...(selectedTools ? { tools: selectedTools } : {}),
    };
  }

  const config: CallmuxConfig = {
    ...(existing ?? {}),
    servers,
    cacheTtlSeconds: cacheTtl,
    ...(!existing ? { maxConcurrency: 20 } : {}),
  };

  // Cache and meta-only choices are always explicitly prompted. Preserve every
  // other existing option, including options added in future releases.
  if (metaOnly) {
    config.metaOnly = true;
    // Meta-only needs the callmux meta-tools to remain visible. Omitting this
    // field restores its default (true) if the previous config hid meta-tools.
    delete config.exposeMetaTools;
    if (descriptionMaxLength !== undefined) {
      config.descriptionMaxLength = descriptionMaxLength;
    } else {
      delete config.descriptionMaxLength;
    }
  } else {
    delete config.metaOnly;
  }

  return config;
}

async function attachToClients(
  configPath: string,
  clientMode: SetupClientMode
): Promise<void> {
  const clients = await p.multiselect({
    message: "Register callmux in which client(s)?",
    options: [
      { value: "claude", label: "Claude Code", hint: "~/.claude.json" },
      { value: "codex", label: "Codex", hint: "~/.codex/config.toml" },
      { value: "desktop", label: "Claude Desktop", hint: "claude_desktop_config.json" },
    ],
    required: false,
  });

  if (p.isCancel(clients) || clients.length === 0) {
    p.log.info("Skipped client registration. Run `callmux client attach <client>` later.");
    return;
  }

  for (const client of clients) {
    if (client === "desktop") {
      p.log.info("Claude Desktop: add callmux manually to claude_desktop_config.json (see README).");
      continue;
    }

    const kind = client as ClientKind;
    const filePath = getDefaultClientConfigPath(kind);

    try {
      let source = "";
      try {
        source = await readFile(filePath, "utf-8");
      } catch {
        // File doesn't exist yet — will be created
      }

      const mutate = kind === "claude" ? attachClaudeConfig : attachCodexConfig;
      const result = mutate({
        source,
        configPath,
        serverName: "callmux",
        ...(clientMode.mode === "shared"
          ? { url: listenerClientUrl(clientMode.listenerUrl, kind) }
          : {}),
      });

      if (result.changed) {
        await mkdir(dirname(filePath), { recursive: true });
        await writeFile(filePath, result.content, "utf-8");
        p.log.success(`Attached to ${kind === "claude" ? "Claude Code" : "Codex"} (${filePath})`);
      } else {
        p.log.info(`${kind === "claude" ? "Claude Code" : "Codex"} already configured.`);
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      p.log.error(`Failed to attach to ${kind}: ${msg}`);
    }
  }
}
