# GPT Runtime Verification

Date: 2026-08-23

## Runtime Rules

- A model is GPT when its model ID matches `/^gpt-/i`. The check reads only the model object's `id`.
- `pi-observational-memory` exits before its compaction trigger and consolidation launcher for GPT models. GPT sessions therefore do not start the observer, reflector, or dropper and do not run local observational-memory compaction.
- `pi-remote-compaction-v2` handles eligible GPT models through the active provider and model registry. It requires `api === "openai-responses"`, preserves the provider base URL, headers, and authentication, and posts to `/responses` with the remote compaction trigger.
- GPT Responses requests default `reasoning.summary` to `detailed`. Responses reasoning deltas arriving before `output_item.added` are buffered, so Pi receives a `thinking` block instead of losing the summary.
- `hideThinkingBlock` remains `false` in the active settings.

## TUI Ownership

- `pi-mcp-adapter` owns fastctx and other MCP/foreign tool rendering. Active `C:\Users\Admin\.pi\agent\mcp.json` uses compact rendering with one collapsed result line.
- `session_pool` now owns its own compact renderer in `packages/coding-agent/examples/extensions/session-pool/index.ts`; expanded output remains available through the normal tool expansion action.
- `pi-tidy-tools` intentionally owns only the seven built-in tools. Active settings now select the local fork at `C:\Users\Admin\Projects\AI\pi-tidy-tools`; the npm copy is no longer listed.

## Actual Global Runtime

- Launcher: `C:\Users\Admin\scoop\apps\nodejs-lts\current\bin\pi.ps1`
- CLI: `C:\Users\Admin\scoop\apps\nodejs-lts\current\bin\node_modules\@earendil-works\pi-coding-agent\dist\cli.js`
- Nested runtime package: `C:\Users\Admin\scoop\persist\nodejs-lts\bin\node_modules\@earendil-works\pi-coding-agent\node_modules\@earendil-works\pi-ai`
- `pi --version`: `0.84.2`

The two deployed files are backed up under:

`C:\Users\Admin\.pi\agent\backups\pi-ai-20260823-001824\`

Only `dist/api/openai-responses.js` and `dist/api/openai-responses-shared.js` were replaced. Their SHA-256 hashes match the locally built Pi artifacts.

## Verification Results

- Pi AI Responses regression tests: 46 passed in 2 files.
- Pi AI offline build: passed.
- Session-pool tests: 6 passed.
- Coding-agent build typecheck: passed.
- Observational-memory focused tests: 86 passed in 4 files.
- Observational-memory typecheck: passed.
- Remote-compaction protocol/network tests: 14 passed.
- Remote-compaction loadability/payload tests could not run because that checkout has no installed `@earendil-works/pi-coding-agent` dependency. This is an environment dependency issue, not a test assertion failure.
- The local `pi-tidy-tools` fork now has its lockfile dependencies installed, including `semver`, so the real Pi loader can start it. Its upstream test command runs 198 tests: 192 pass and 6 fail in pre-existing environment/contract cases (temporary settings/reload fixtures and upstream metadata expectations); none touch the GPT Responses or MCP renderer changes.
- `pi --no-extensions --help`: passed.
- Full global RPC extension discovery: passed; the local fork, observational-memory, remote-compaction, and session-pool packages all load and register their commands.
- A fresh no-session `local/gpt-5.6-luna` request: passed through `openai-responses` and returned the expected answer. The local gateway emitted no reasoning summary for that prompt, so summary visibility is covered by the 46 Responses parser tests and the TUI `AssistantMessageComponent` path rather than claimed from an absent server event.

## Accepted Summary Behavior

Reasoning summaries are model/provider output, not a required Pi invariant. A GPT Responses model may return no summary; that is accepted and does not fail the session. Pi only renders a summary when the provider sends one.

## Session Note

The inspected session `01a0290c-3143-7b38-ae28-9d174eb3470f` was created before the global nested `pi-ai` deployment. Its old JSONL cannot gain thinking blocks or remote compaction entries retroactively. Restart Pi and create a new GPT session to verify the deployed runtime; the old session remains useful as the before-fix evidence.
