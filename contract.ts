import { defineRpcContract } from "@get-bb/plugin-sdk";
import { z } from "zod";

/**
 * Shared between server.ts (host RPC caller) and host.ts (host RPC handler +
 * provider bridge). The bridge process is isolated from the server's own
 * settings store, so config reaches it by the server pushing it here after
 * every settings change, and the bridge persists it to its own dataDir.
 */
export const omniRouteHostContract = defineRpcContract({
  setConfig: {
    input: z
      .object({
        baseUrl: z.string().min(1),
        apiKey: z.string(),
        model: z.string().min(1),
      })
      .strict(),
    output: z.object({ ok: z.literal(true) }).strict(),
  },
});
