/**
 * The plugin's single bb.host artifact. Two independent consumers:
 * - `experimental_providerBridge` — run by the daemon's bridge bootstrap, one
 *   process per thread on the "omniroute" provider. Forwards each turn to a
 *   real OmniRoute instance's OpenAI-compatible /api/v1/chat/completions.
 *   Protocol shape follows bb's own examples/plugins/echo-provider at the
 *   desktop-v0.39.0 tag (matching this bb's SDK 0.4.8): the bridge mints
 *   turn/item ids and emits full thread/event notifications directly — no
 *   thread/delta grammar in this SDK version.
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
  type ThreadEvent,
  BRIDGE_JSON_RPC_ERRORS,
  BRIDGE_NOTIFICATION_METHODS,
  BRIDGE_REQUEST_METHODS,
  PROVIDER_BRIDGE_PROTOCOL_VERSION,
  initializeParamsSchema,
  modelListParamsSchema,
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
let turnCounter = 0;
const sessions = new Map<string, string>();

type JsonRpcId = string | number;

function writeMessage(message: Record<string, unknown>): void {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
}
function respondResult(id: JsonRpcId, result: unknown): void {
  writeMessage({ id, result });
}
function respondError(id: JsonRpcId, code: number, message: string, data?: unknown): void {
  writeMessage({ id, error: { code, message, ...(data !== undefined ? { data } : {}) } });
}
function notify(method: string, params: Record<string, unknown>): void {
  writeMessage({ method, params });
}
function emitThreadEvent(threadId: string, event: ThreadEvent): void {
  notify(BRIDGE_NOTIFICATION_METHODS.threadEvent, { threadId, event });
}

function promptText(input: readonly PromptInput[]): string {
  return input
    .filter((item): item is Extract<PromptInput, { type: "text" }> => item.type === "text")
    .map((item) => item.text)
    .join("");
}

/** One non-streaming call to OmniRoute's OpenAI-compatible endpoint. */
async function callOmniRoute(
  config: OmniRouteConfig,
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
        model: config.model,
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
  input: readonly PromptInput[];
  clientRequestId?: string;
}): Promise<void> {
  turnCounter += 1;
  const turnId = `turn_omniroute_${instanceNonce}_${turnCounter}`;
  const itemId = `${turnId}_item_1`;
  const scope = { kind: "turn", turnId } as const;
  const base = { threadId: args.threadId, providerThreadId: args.providerThreadId };

  // Empirically, this server (bb 0.39.0) rejects turn/input/accepted with a
  // 409 ("before turn/started is stored") when emitted ahead of turn/started
  // — the reverse of the shipped echo-provider example at the matching tag.
  // Verified against a live instance: turn/started must be stored first.
  emitThreadEvent(args.threadId, { type: "turn/started", ...base, scope });
  if (args.clientRequestId !== undefined) {
    emitThreadEvent(args.threadId, {
      type: "turn/input/accepted",
      ...base,
      clientRequestId: args.clientRequestId,
      scope,
    });
  }
  emitThreadEvent(args.threadId, {
    type: "item/started",
    ...base,
    item: { type: "agentMessage", id: itemId, text: "" },
    scope,
  });

  const config = loadConfig();
  const result = config
    ? await callOmniRoute(config, promptText(args.input))
    : { error: "OmniRoute is not configured — set baseUrl/apiKey in plugin settings." };

  const text = "error" in result ? `OmniRoute request failed: ${result.error}` : result.text;
  emitThreadEvent(args.threadId, {
    type: "item/agentMessage/delta",
    ...base,
    itemId,
    delta: text,
    scope,
  });
  emitThreadEvent(args.threadId, {
    type: "item/completed",
    ...base,
    item: { type: "agentMessage", id: itemId, text },
    scope,
  });
  emitThreadEvent(args.threadId, {
    type: "turn/completed",
    ...base,
    status: "error" in result ? "failed" : "completed",
    scope,
  });
}

type RequestHandler = (id: JsonRpcId, params: unknown) => void;

function invalidParams(id: JsonRpcId, method: string, issues: unknown): void {
  respondError(id, BRIDGE_JSON_RPC_ERRORS.INVALID_PARAMS, `Invalid params for ${method}`, issues);
}

const handlers: Record<string, RequestHandler> = {
  [BRIDGE_REQUEST_METHODS.initialize]: (id, params) => {
    const parsed = initializeParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.initialize, parsed.error.issues);
      return;
    }
    respondResult(id, { protocolVersion: PROVIDER_BRIDGE_PROTOCOL_VERSION, capabilities: {} });
  },

  [BRIDGE_REQUEST_METHODS.modelList]: (id, params) => {
    const parsed = modelListParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.modelList, parsed.error.issues);
      return;
    }
    // OmniRoute fronts hundreds of models across providers; a single default
    // entry (the configured model/combo, e.g. "auto/smart") lets bb resolve
    // a default without --model on every spawn, rather than enumerating a
    // live catalog here.
    respondResult(id, {
      models: [
        {
          id: "default",
          model: "default",
          displayName: "OmniRoute (configured default)",
          description: "Uses this plugin's configured default model/combo.",
          supportedReasoningEfforts: [
            { reasoningEffort: "low", description: "Low" },
            { reasoningEffort: "medium", description: "Medium" },
            { reasoningEffort: "high", description: "High" },
          ],
          defaultReasoningEffort: "medium",
          isDefault: true,
        },
      ],
      selectedOnlyModels: [],
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
    sessions.set(parsed.data.threadId, providerThreadId);
    notify(BRIDGE_NOTIFICATION_METHODS.threadIdentity, {
      threadId: parsed.data.threadId,
      providerThreadId,
    });
    respondResult(id, { providerThreadId });
    if (parsed.data.input !== undefined && parsed.data.input.length > 0) {
      void runTurn({ threadId: parsed.data.threadId, providerThreadId, input: parsed.data.input });
    }
  },

  [BRIDGE_REQUEST_METHODS.threadResume]: (id, params) => {
    const parsed = threadResumeParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.threadResume, parsed.error.issues);
      return;
    }
    sessions.set(parsed.data.threadId, parsed.data.providerThreadId);
    notify(BRIDGE_NOTIFICATION_METHODS.threadIdentity, {
      threadId: parsed.data.threadId,
      providerThreadId: parsed.data.providerThreadId,
    });
    respondResult(id, { providerThreadId: parsed.data.providerThreadId });
  },

  [BRIDGE_REQUEST_METHODS.turnStart]: (id, params) => {
    const parsed = turnStartParamsSchema.safeParse(params);
    if (!parsed.success) {
      invalidParams(id, BRIDGE_REQUEST_METHODS.turnStart, parsed.error.issues);
      return;
    }
    respondResult(id, {});
    void runTurn({
      threadId: parsed.data.threadId,
      providerThreadId: parsed.data.providerThreadId,
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
    respondError(
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
    respondResult(id, {});
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
    respondError(id, BRIDGE_JSON_RPC_ERRORS.METHOD_NOT_FOUND, `Method not found: ${method}`);
    return;
  }
  handler(id, params);
}

export const experimental_providerBridge = experimental_defineProviderBridge({
  handleLine,
});
