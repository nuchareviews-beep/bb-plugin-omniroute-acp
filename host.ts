/**
 * The plugin's single bb.host artifact. Two independent consumers:
 * - `experimental_providerBridge` — run by the daemon's bridge bootstrap, one
 *   process per thread on the "omniroute" provider. Forwards each turn to a
 *   real OmniRoute instance's OpenAI-compatible /api/v1/chat/completions.
 *   Protocol shape follows BB 0.40's examples/plugins/echo-provider: the
 *   bridge emits semantic thread/delta batches and the runtime owns timeline
 *   event and identifier assembly.
 * - `default` (host RPC entry) — run by the daemon's host worker. Its only
 *   job is `setConfig`: the server pushes settings here because the bridge
 *   process has no access to bb.settings of its own.
 *
 * Minimal but honest: single-shot, non-streaming turns (one request per
 * turn/start, no mid-turn tool calls, no steer support).
 */
import { experimental_defineHostEntry } from "@get-bb/plugin-sdk/host";
import {
  type PromptInput,
  type ThreadDelta,
  BRIDGE_JSON_RPC_ERRORS,
  BRIDGE_NOTIFICATION_METHODS,
  BRIDGE_REQUEST_METHODS,
  PROVIDER_BRIDGE_PROTOCOL_VERSION,
  THREAD_DELTA_GRAMMAR_V3,
  THREAD_DELTA_NOTIFICATION_METHOD,
  createBridgeIo,
  initializeParamsSchema,
  modelListParamsSchema,
  runBridgeRequest,
  threadResumeParamsSchema,
  threadStartParamsSchema,
  threadStopParamsSchema,
  turnStartParamsSchema,
  turnSteerParamsSchema,
  experimental_defineProviderBridge,
} from "@get-bb/plugin-sdk/provider-bridge";
import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { omniRouteHostContract } from "./contract.js";

// ---------------------------------------------------------------------------
// Host RPC entry: the server's only way to hand this isolated process config.
//
// The host RPC entry and the provider bridge are two independent worker
// processes for the same bb.host artifact, and — empirically, verified
// against a live bb 0.39.0 daemon — each gets its OWN per-consumer dataDir
// ("host-data/" vs "bridge-data/" under the plugin's data directory), not a
// shared one, despite both being "this plugin on this daemon". So config
// can't be handed off through either context's dataDir; it goes through a
// fixed OS-temp-dir path instead, which both processes share because both
// run on the same host machine as the daemon that spawned them.
// ---------------------------------------------------------------------------

const configPath = join(tmpdir(), "bb-plugin-omniroute-acp-config.json");

export default experimental_defineHostEntry({
  contract: omniRouteHostContract,
  handlers: {
    setConfig: (input) => {
      writeFileSync(configPath, JSON.stringify(input));
      return { ok: true as const };
    },
  },
});

// ---------------------------------------------------------------------------
// Provider bridge state
// ---------------------------------------------------------------------------

interface OmniRouteConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}

function loadConfig(): OmniRouteConfig | null {
  if (!configPath || !existsSync(configPath)) return null;
  try {
    return JSON.parse(readFileSync(configPath, "utf8")) as OmniRouteConfig;
  } catch {
    return null;
  }
}

const instanceNonce = randomUUID().replaceAll("-", "").slice(0, 12);
let threadCounter = 0;
/** threadId -> { providerThreadId, model } — model is frozen at thread construction. */
const sessions = new Map<string, { providerThreadId: string; model: string }>();

type JsonRpcId = string | number;

type OutboundMessage = { jsonrpc: "2.0" } & Record<string, unknown>;
const io = createBridgeIo<OutboundMessage>();

function notify(method: string, params: Record<string, unknown>): void {
  io.send({ jsonrpc: "2.0", method, params });
}
function emitDeltas(threadId: string, deltas: ThreadDelta[]): void {
  notify(THREAD_DELTA_NOTIFICATION_METHOD, { threadId, deltas });
}

function promptText(input: readonly PromptInput[]): string {
  return input
    .filter((item): item is Extract<PromptInput, { type: "text" }> => item.type === "text")
    .map((item) => item.text)
    .join("");
}

// ---------------------------------------------------------------------------
// Live model catalog: OmniRoute's /api/v1/models lists ~500 individual
// provider models plus its named auto-routing combos (owned_by: "combo").
// The picker shows combos, not the raw provider catalog — a combo is what a
// user actually picks (e.g. "auto/coding", "auto/cheap"), and OmniRoute
// resolves it to a concrete model per request.
// ---------------------------------------------------------------------------

interface OmniRouteModel {
  id: string;
  model: string;
  displayName: string;
  description: string;
  supportedReasoningEfforts: { reasoningEffort: "low" | "medium" | "high"; description: string }[];
  defaultReasoningEffort: "low" | "medium" | "high";
  isDefault: boolean;
}

let modelCache: { at: number; models: OmniRouteModel[] } | null = null;
const MODEL_CACHE_TTL_MS = 5 * 60_000;

function prettifyComboId(id: string): string {
  const withoutPrefix = id.startsWith("auto/") ? id.slice("auto/".length) : id;
  return withoutPrefix
    .split(/[-:]/)
    .filter(Boolean)
    .map((word) => word[0]!.toUpperCase() + word.slice(1))
    .join(" ");
}

async function fetchComboModels(config: OmniRouteConfig): Promise<OmniRouteModel[]> {
  if (modelCache && Date.now() - modelCache.at < MODEL_CACHE_TTL_MS) return modelCache.models;
  try {
    const res = await fetch(`${config.baseUrl}/api/v1/models`, {
      headers: config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {},
      signal: AbortSignal.timeout(10_000),
    });
    if (!res.ok) return modelCache?.models ?? [];
    const data = (await res.json()) as { data?: { id: string; owned_by?: string }[] };
    const combos = (data.data ?? []).filter((m) => m.owned_by === "combo");
    const models: OmniRouteModel[] = combos.map((combo) => ({
      id: combo.id,
      model: combo.id,
      displayName: prettifyComboId(combo.id),
      description: `OmniRoute auto-routing combo (${combo.id}) — picks the best available provider/model for this category automatically.`,
      // OmniRoute combo ids (auto/smart, auto/coding, ...) already encode
      // routing intent; there's no separate reasoning-effort request field
      // for them, so a single fixed value keeps BB from showing a picker
      // that would have no effect on the actual request.
      supportedReasoningEfforts: [{ reasoningEffort: "medium", description: "Standard" }],
      defaultReasoningEffort: "medium",
      isDefault: combo.id === config.model,
    }));
    if (models.length > 0 && !models.some((m) => m.isDefault)) models[0]!.isDefault = true;
    modelCache = { at: Date.now(), models };
    return models;
  } catch {
    return modelCache?.models ?? [];
  }
}

/** One non-streaming call to OmniRoute's OpenAI-compatible endpoint. */
async function callOmniRoute(
  config: OmniRouteConfig,
  model: string,
  prompt: string,
): Promise<{ text: string } | { error: string }> {
  try {
    const res = await fetch(`${config.baseUrl}/api/v1/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...(config.apiKey ? { authorization: `Bearer ${config.apiKey}` } : {}),
      },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: prompt }],
        stream: false,
      }),
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      return { error: `OmniRoute ${res.status}: ${body.slice(0, 500)}` };
    }
    const data = (await res.json()) as { choices?: { message?: { content?: string } }[] };
    return { text: data.choices?.[0]?.message?.content ?? "" };
  } catch (err) {
    return { error: err instanceof Error ? err.message : String(err) };
  }
}

async function runTurn(args: {
  threadId: string;
  providerThreadId: string;
  model: string;
  input: readonly PromptInput[];
  clientRequestId?: string;
}): Promise<void> {
  const itemId = `omniroute_${args.providerThreadId}_${randomUUID()}`;
  const deltas: ThreadDelta[] = [];
  if (args.clientRequestId !== undefined) {
    deltas.push({
      kind: "input.accepted",
      clientRequestId: args.clientRequestId,
    });
  }
  deltas.push({ kind: "turn.open" });
  emitDeltas(args.threadId, deltas);

  const config = loadConfig();
  const result = config
    ? await callOmniRoute(config, args.model, promptText(args.input))
    : { error: "OmniRoute is not configured — set baseUrl/apiKey in plugin settings." };

  const text = "error" in result ? `OmniRoute request failed: ${result.error}` : result.text;
  emitDeltas(args.threadId, [
    {
      kind: "item.open",
      key: { providerItemId: itemId },
      item: { type: "agentMessage", text: "" },
    },
    {
      kind: "item.textClose",
      key: { providerItemId: itemId },
      channel: "agentMessage",
      text,
    },
    { kind: "turn.boundary", status: "error" in result ? "failed" : "completed" },
  ]);
}

type RequestHandler = (id: JsonRpcId, params: unknown) => void;

function invalidParams(id: JsonRpcId, method: string, issues: unknown): void {
  io.sendError(id, BRIDGE_JSON_RPC_ERRORS.INVALID_PARAMS, `Invalid params for ${method}`, issues);
}

const handlers: Record<string, RequestHandler> = {
  [BRIDGE_REQUEST_METHODS.initialize]: (id, params) => {
    const parsed = initializeParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.initialize, parsed.error.issues);
      return;
    }
    io.sendResult(id, {
      protocolVersion: PROVIDER_BRIDGE_PROTOCOL_VERSION,
      capabilities: {
        grammarVersions: [THREAD_DELTA_GRAMMAR_V3, THREAD_DELTA_GRAMMAR_V3],
        sessionRestore: false,
        threadArchive: false,
        threadRename: false,
        threadGoalClear: false,
        fork: "none",
        approvalEnforcedBy: "runtime",
        steerMode: "queue",
      },
    });
  },

  [BRIDGE_REQUEST_METHODS.modelList]: (id, params) => {
    const parsed = modelListParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.modelList, parsed.error.issues);
      return;
    }
    const config = loadConfig() ?? { baseUrl: "http://localhost:20128", apiKey: "", model: "auto/smart" };
    void fetchComboModels(config).then((models) => {
      io.sendResult(id, { models, selectedOnlyModels: [] });
    });
  },

  [BRIDGE_REQUEST_METHODS.threadStart]: (id, params) => {
    const parsed = threadStartParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.threadStart, parsed.error.issues);
      return;
    }
    threadCounter += 1;
    const providerThreadId = `omniroute_${instanceNonce}_${threadCounter}`;
    const config = loadConfig();
    const model = parsed.data.options?.model || config?.model || "auto/smart";
    sessions.set(parsed.data.threadId, { providerThreadId, model });
    notify(BRIDGE_NOTIFICATION_METHODS.threadIdentity, {
      threadId: parsed.data.threadId,
      providerThreadId,
    });
    emitDeltas(parsed.data.threadId, [{ kind: "session.reset" }]);
    io.sendResult(id, { providerThreadId, sessionRestorable: false });
    if (parsed.data.input !== undefined && parsed.data.input.length > 0) {
      void runTurn({ threadId: parsed.data.threadId, providerThreadId, model, input: parsed.data.input });
    }
  },

  [BRIDGE_REQUEST_METHODS.threadResume]: (id, params) => {
    const parsed = threadResumeParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.threadResume, parsed.error.issues);
      return;
    }
    const config = loadConfig();
    const model = parsed.data.options?.model || config?.model || "auto/smart";
    sessions.set(parsed.data.threadId, { providerThreadId: parsed.data.providerThreadId, model });
    notify(BRIDGE_NOTIFICATION_METHODS.threadIdentity, {
      threadId: parsed.data.threadId,
      providerThreadId: parsed.data.providerThreadId,
    });
    emitDeltas(parsed.data.threadId, [{ kind: "session.reset" }]);
    io.sendResult(id, { providerThreadId: parsed.data.providerThreadId, sessionRestorable: false });
  },

  [BRIDGE_REQUEST_METHODS.turnStart]: (id, params) => {
    const parsed = turnStartParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.turnStart, parsed.error.issues);
      return;
    }
    const session = sessions.get(parsed.data.threadId);
    if (session === undefined) {
      io.sendError(id, BRIDGE_JSON_RPC_ERRORS.INVALID_PARAMS, `No session for thread ${parsed.data.threadId}; send thread/start or thread/resume first`);
      return;
    }
    io.sendResult(id, {});
    const config = loadConfig();
    const model = parsed.data.options?.model || session?.model || config?.model || "auto/smart";
    void runTurn({
      threadId: parsed.data.threadId,
      providerThreadId: parsed.data.providerThreadId,
      model,
      input: parsed.data.input,
      clientRequestId: parsed.data.clientRequestId,
    });
  },

  [BRIDGE_REQUEST_METHODS.turnSteer]: (id, params) => {
    const parsed = turnSteerParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.turnSteer, parsed.error.issues);
      return;
    }
    io.sendError(
      id,
      BRIDGE_JSON_RPC_ERRORS.NO_ACTIVE_TURN,
      `No active turn to steer (expected ${parsed.data.expectedTurnId})`,
    );
  },

  [BRIDGE_REQUEST_METHODS.threadStop]: (id, params) => {
    const parsed = threadStopParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.threadStop, parsed.error.issues);
      return;
    }
    sessions.delete(parsed.data.threadId);
    io.sendResult(id, {});
  },
};

export function handleLine(line: string): void {
  let message: unknown;
  try {
    message = JSON.parse(line);
  } catch {
    return;
  }
  if (typeof message !== "object" || message === null || Array.isArray(message)) {
    return;
  }
  const { id, method, params } = message as { id?: unknown; method?: unknown; params?: unknown };
  if (typeof method !== "string") return;
  if (typeof id !== "string" && typeof id !== "number") return;
  const handler = handlers[method];
  if (handler === undefined) {
    io.sendError(id, BRIDGE_JSON_RPC_ERRORS.METHOD_NOT_FOUND, `Method not found: ${method}`);
    return;
  }
  void runBridgeRequest({
    request: { id, method, params },
    sendError: io.sendError,
    handleRequest: async (request) => handler(request.id, request.params),
  });
}

export const experimental_providerBridge = experimental_defineProviderBridge({
  handleLine,
});
