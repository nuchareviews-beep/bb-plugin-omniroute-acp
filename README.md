# bb-plugin-omniroute-acp

Bridges BB to a locally-running [OmniRoute](https://github.com/diegosouzapw/OmniRoute) instance and registers it as a BB agent provider.

## What it does

- Adds **OmniRoute** to the BB provider picker, with a real model dropdown backed by OmniRoute's own `/api/v1/models` — it lists OmniRoute's auto-routing combos (`auto/smart`, `auto/coding`, `auto/cheap`, etc.), not a single placeholder entry. Threads created on this provider forward each turn to OmniRoute's OpenAI-compatible `/api/v1/chat/completions`, so any of the ~300 upstream providers/models OmniRoute fronts can be used from inside BB.
- Polls OmniRoute's own `/api/usage/analytics` and `/api/usage/call-logs` on a 5-minute background service and exposes the latest snapshot via a `usage` RPC method (bb-plugin-usage has no ingestion API for other plugins to push into, so this plugin owns its own metrics surface).
- Registers an `omniswarm_spawn` native tool that spawns a batch of hidden subagent threads on the OmniRoute provider (or another named provider), so many models can work a task list in parallel. **Work in progress** — it exists and runs, but hasn't been exercised or tuned for real multi-task fan-out workloads yet; treat it as an early draft, not a finished feature.

## Requirements

- A reachable OmniRoute instance. The plugin defaults to `http://localhost:20128`; set another URL with `bb plugin config omniroute-acp set baseUrl <url>`.
- If OmniRoute's `REQUIRE_API_KEY` is on, set an API key too: `bb plugin config omniroute-acp set apiKey <key>` (stored as a secret, never leaves the server).

## Install

```sh
bb plugin install git:https://github.com/nuchareviews-beep/bb-plugin-omniroute-acp.git@^0.1.0
```

## Settings

- `baseUrl` (default `http://localhost:20128`) — OmniRoute base URL
- `apiKey` (secret) — OmniRoute bearer token if that instance requires one
- `model` (default `auto/smart`) — default model or combo id the picker uses

## Scope

Single-shot, non-streaming turns (one request per `turn/start`, no mid-turn tool calls, no steer). Sufficient to route chat completions through OmniRoute end-to-end; streaming and provider-native tool calls are out of scope for this release.

## Related

- [bb-plugin-antigravity-acp](https://github.com/nuchareviews-beep/bb-plugin-antigravity-acp) — sibling plugin bridging BB to the local Antigravity CLI (`agy`).

## License

[MIT](LICENSE)
