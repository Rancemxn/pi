# Session Pool Extension

Coordinate independent Pi sessions from one coordinator session. The extension
uses a small file mailbox under the Pi agent directory, so it needs no daemon,
database, or extra dependency.

## Load

Load it explicitly in every TUI or RPC session:

```text
pi --extension packages/coding-agent/examples/extensions/session-pool/index.ts
```

For automatic loading, copy or symlink the directory to
`~/.pi/agent/extensions/session-pool/` so that it contains `index.ts` and
`store.ts`.

On Windows, the equivalent PowerShell command is:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.pi\agent\extensions\session-pool"
Copy-Item packages\coding-agent\examples\extensions\session-pool\*.ts "$env:USERPROFILE\.pi\agent\extensions\session-pool\"
```

## Manual sessions

In a session that should be discoverable, run:

```text
/pool-ready worker-a
/pool-meta role=worker task="review the parser" worktree=C:\work\parser-a
```

Use `/pool-unready` to hide it. Ready state and metadata are stored in the Pi
session branch and survive an extension/session reload. `/pool-status` and the
TUI widget show local sessions, ownership, status, and active monitors.

## Automatic children

The coordinator enables a launch mode once:

```text
/pool-mode visible
```

`visible` launches a new Windows Terminal tab/window through `wt.exe`. If the
launcher is unavailable, the extension falls back to a headless RPC child and
reports the reason. `headless` always starts `pi --mode rpc` without a window.
`off` disables automatic spawning. A managed child is ready immediately and
cannot recursively spawn more children.

## Model tool

The `session_pool` tool is intentionally a transport/state layer. The
coordinator model chooses roles such as worker, planner, or reviewer, prompts,
models, thinking levels, concurrency, and workflow.

Typical flow:

1. `list` finds fresh, ready, idle sessions in the same normalized `cwd`.
2. `spawn` optionally creates a visible or headless managed child.
3. `assign` claims a session, applies coordinator metadata, and queues its
   prompt. Claiming is serialized by the child mailbox; a competing claim is
   rejected.
4. `send`, `abort`, `update_meta`, `release`, and `stop` control claimed children.
5. `monitor_start` records assignment IDs and a model-selected timeout.
6. `monitor_status` reads progress without holding a model turn open.

`report` is available inside any child after it has been claimed. Use `reportKind` values
`progress`, `needs_attention`, `needs_decision`, or `failed`. Progress updates
are retained in assignment/monitor status; attention, decision, and failure
reports are also delivered according to the monitor wake policy.

## Waiting and wake policies

Monitors are programmatic timers. The coordinator tool call returns after the
monitor is written; it does not spend tokens waiting for child completion.

- `queue` (default) puts completion/report messages in the coordinator's
  `nextTurn` queue with `triggerTurn: false`. They are consumed on the next
  user/model turn.
- `wake` explicitly triggers a coordinator turn when a report or monitor
  transition arrives.
- `silent` records state but does not enqueue a message.

The timeout only changes the monitor to `timed_out`; it does not kill a child.
Use `stop` or `send` when the workflow decides what to do next.

## Scope and limitations

- Discovery is local to the same OS user and normalized working directory.
- The extension does not create or manage worktrees, branches, or file locks;
  the user/workflow must provide isolation.
- Visible launch currently targets Windows Terminal (`wt.exe`).
- Registry files are heartbeat-based and stale records are ignored on reads.
- A coordinator shutdown asks its managed children to stop; stale owner leases
  also make managed children exit after the heartbeat grace period.

Shared state is stored at `<pi agent dir>/session-pool/` (or the directory set
by `PI_SESSION_POOL_DIR`) with separate session, command, event, assignment,
and monitor files.
