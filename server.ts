// bb-plugin-omniroute-acp — registers OmniRoute as a BB agent provider,
// exposes its usage/cost metrics via RPC, and provides an omniswarm_spawn
// tool for fanning out subagent threads across it.
import { defineRpcContract, type BbPluginApi } from "@get-bb/plugin-sdk";
import { z } from "zod";
import { omniRouteHostContract } from "./contract.js";

export const rpcContract = defineRpcContract({
  usage: {
    input: z.null(),
    output: z.object({
      updatedAt: z.number().nullable(),
      analytics: z.unknown().nullable(),
      recentCallLogs: z.unknown().nullable(),
      lastError: z.string().nullable(),
    }),
  },
});

export default async function plugin(bb: BbPluginApi) {
  const settings = bb.settings.define({
    baseUrl: { type: "string", label: "OmniRoute base URL", default: "http://localhost:20128" },
    apiKey: { type: "string", label: "OmniRoute API key", secret: true },
    model: { type: "string", label: "Default model / combo", default: "auto/smart" },
  });

  const hostClient = bb.hosts.experimental_client({ contract: omniRouteHostContract });

  async function primaryHostId(): Promise<string | undefined> {
    const hosts = await bb.sdk.hosts.list();
    return hosts[0]?.id;
  }

  async function pushConfigToHost(): Promise<void> {
    const { baseUrl, apiKey, model } = await settings.get();
    const hostId = await primaryHostId();
    if (!hostId) {
      bb.log.warn("no enrolled host to push OmniRoute config to");
      return;
    }
    try {
      await hostClient.call("setConfig", { baseUrl, apiKey: apiKey ?? "", model }, { hostId });
    } catch (err) {
      bb.log.error(`failed to push config to host: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // Host RPC calls are rejected during factory registration ("host plugin
  // calls are unavailable during factory registration") — defer the initial
  // push to a timer tick, same as any handler/service/timer context.
  setTimeout(() => void pushConfigToHost(), 0);
  settings.onChange(() => {
    void pushConfigToHost();
  });

  // Register OmniRoute as a picker-visible agent provider. The bridge that
  // actually services turns lives in host.ts (bb.host), forwarding each turn
  // to OmniRoute's OpenAI-compatible /api/v1/chat/completions.
  bb.agents.experimental_registerProvider({
    id: "omniroute",
    displayName: "OmniRoute",
    icon: "Waypoints",
    capabilities: {
      supportsServiceTier: false,
      supportsNativeUserQuestion: false,
      fork: "none",
      supportsManualCompaction: false,
      supportsThreadArchive: false,
      supportsThreadRename: false,
      supportsWorkflows: false,
      permissionModes: ["full"],
      reasoningLevels: ["medium"],
    },
    composerActions: [],
  });

  // omniswarm_spawn: fan a batch of prompts out to hidden subagent threads
  // running on the omniroute provider (or whichever providerId is given),
  // so many OmniRoute-routed models can work a task list in parallel.
  bb.agents.registerTool({
    name: "omniswarm_spawn",
    description:
      "(Work in progress) Spawn multiple hidden subagent threads in parallel, each running one task, on the OmniRoute provider (or another installed provider). Returns the spawned thread ids.",
    instructions:
      "Use omniswarm_spawn to fan out independent, parallelizable tasks across OmniRoute-routed models instead of doing them serially in this thread.",
    parameters: z.object({
      tasks: z
        .array(z.object({ prompt: z.string().min(1), title: z.string().optional() }))
        .min(1)
        .max(20),
      providerId: z.string().optional(),
      projectId: z.string().optional(),
    }),
    async execute({ tasks, providerId, projectId: explicitProjectId }, { projectId: contextProjectId }) {
      const projectId = explicitProjectId ?? contextProjectId;
      if (!projectId) {
        return { content: [{ type: "text", text: "No projectId available to spawn into." }], isError: true };
      }
      const spawned: { threadId: string; title?: string }[] = [];
      for (const task of tasks) {
        const thread = await bb.sdk.threads.spawn({
          projectId,
          environment: { type: "project-default" },
          prompt: task.prompt,
          title: task.title,
          providerId: (providerId ?? "omniroute") as never,
          visibility: "hidden",
        });
        spawned.push({ threadId: thread.id, title: task.title });
      }
      return spawned.map((s) => `${s.threadId}${s.title ? ` — ${s.title}` : ""}`).join("\n");
    },
  });

  // Usage/cost metrics: OmniRoute exposes these over its own HTTP API
  // (GET /api/usage/analytics, /api/usage/call-logs) — poll directly from
  // this server-side process rather than through the isolated bridge worker.
  // NOTE: bb-plugin-usage (MayankBansal12/bb-plugin-usage) has no ingestion
  // API for other plugins to push data into, so this is stored and exposed
  // via this plugin's own RPC rather than forwarded into that plugin.
  const db = bb.storage.database();
  bb.storage.migrate(db, [
    `CREATE TABLE IF NOT EXISTS usage_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      fetched_at INTEGER NOT NULL,
      analytics_json TEXT,
      call_logs_json TEXT,
      error TEXT
    )`,
  ]);

  async function fetchUsageOnce(): Promise<void> {
    const { baseUrl, apiKey } = await settings.get();
    const headers: Record<string, string> = apiKey ? { authorization: `Bearer ${apiKey}` } : {};
    try {
      const [analyticsRes, callLogsRes] = await Promise.all([
        fetch(`${baseUrl}/api/usage/analytics?period=day`, { headers }),
        fetch(`${baseUrl}/api/usage/call-logs?limit=20&offset=0`, { headers }),
      ]);
      const analytics = analyticsRes.ok ? await analyticsRes.json() : null;
      const callLogs = callLogsRes.ok ? await callLogsRes.json() : null;
      db.prepare(
        `INSERT INTO usage_snapshots (fetched_at, analytics_json, call_logs_json, error) VALUES (?, ?, ?, ?)`,
      ).run(Date.now(), JSON.stringify(analytics), JSON.stringify(callLogs), null);
    } catch (err) {
      db.prepare(
        `INSERT INTO usage_snapshots (fetched_at, analytics_json, call_logs_json, error) VALUES (?, ?, ?, ?)`,
      ).run(Date.now(), null, null, err instanceof Error ? err.message : String(err));
    }
  }

  bb.background.service("usage-poll", {
    async start(signal) {
      while (!signal.aborted) {
        await fetchUsageOnce();
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, 5 * 60_000);
          signal.addEventListener("abort", () => { clearTimeout(timer); resolve(); }, { once: true });
        });
      }
    },
  });

  bb.rpc.register(rpcContract, {
    usage: () => {
      const row = db
        .prepare(`SELECT fetched_at, analytics_json, call_logs_json, error FROM usage_snapshots ORDER BY id DESC LIMIT 1`)
        .get() as { fetched_at: number; analytics_json: string | null; call_logs_json: string | null; error: string | null } | undefined;
      if (!row) return { updatedAt: null, analytics: null, recentCallLogs: null, lastError: null };
      return {
        updatedAt: row.fetched_at,
        analytics: row.analytics_json ? JSON.parse(row.analytics_json) : null,
        recentCallLogs: row.call_logs_json ? JSON.parse(row.call_logs_json) : null,
        lastError: row.error,
      };
    },
  });

  const { baseUrl, apiKey } = await settings.get();
  if (!apiKey) {
    bb.log.warn(
      "no OmniRoute API key set — fine if that instance's REQUIRE_API_KEY is false, otherwise set one with `bb plugin config omniroute-acp set apiKey <key>`",
    );
  }
  bb.log.info(`omniroute-acp loaded, targeting ${baseUrl}`);

  bb.onDispose(() => {
    bb.log.info("disposed");
  });
}
