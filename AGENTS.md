# Interruptus — Agent Context

## Mission

Interruptus is an Elixir library that provides **durable workflow pipelines** with **typed params and data**, **checkpoint-based durability**, **multi-node exclusivity**, and **explicit suspend/resume**. It is a simpler, in-process alternative to Temporal: workflows run on the BEAM, persist state in the host application's PostgreSQL database, and resume after crashes, restarts, or voluntary suspension without requiring an external orchestrator.

## Architecture Map

```
Host Application
├── {Interruptus, repo: MyApp.Repo}     # OTP child under application supervisor
└── MyApp.Repo                        # Host-owned Ecto repo

Interruptus OTP Tree (per instance, names derived from config :name)
├── Interruptus.Supervisor
│   ├── Interruptus.Registry          # workflow_id → Runner pid
│   ├── Interruptus.TaskSupervisor    # Task.Supervisor executing stage segments
│   ├── Interruptus.RunnerSupervisor  # DynamicSupervisor for runners
│   └── Interruptus.Recovery          # Boot scan + stale lease reclaim (jittered)
└── Interruptus.Runner (per workflow) # GenServer driving segments + heartbeat

Database (embedded via Interruptus.Migration)
├── interruptus_workflows             # Instance rows, locks, snapshots
├── interruptus_checkpoints           # Historical checkpoint audit trail
├── interruptus_stage_attempts        # Per-stage execution log
└── interruptus_effects               # Idempotent effect markers (workflow_id + key)
```

### Claim / Recover Loop

1. `Interruptus.start/3` inserts a `:pending` workflow row and starts a Runner
   (idempotent when `:idempotency_key` is given — duplicate returns existing row).
2. Runner claims the row (`FOR UPDATE SKIP LOCKED`; bumps `lock_version`).
   A runner that cannot claim stops immediately. A `pipeline_version` mismatch
   parks the row as `:suspended` (`"pipeline_version_mismatch"`).
3. Runner persists `attempt_count + 1` (fenced), then executes the segment in
   a `Task.Supervisor` task: verify → stages → checkpoint → advance. Heartbeats
   renew the lease concurrently with execution. Checkpoints reset the budget.
4. On suspend: persist snapshot, set `:suspended`, release lease, stop process.
5. On crash: lease expires; `Interruptus.Recovery` reclaims (`:pending`,
   `:running`, `:compensating` — never `:suspended`) and starts a new Runner.
6. On failure after retries: compensation runs step-by-step, persisting
   `compensation_index` (crash-resumable); ends `:compensated` or `:failed`.
7. On complete: set `:completed`; Recovery never restarts terminal workflows.

## Shared database notes

- Stages run **outside** Interruptus transactions; stage DB writes and checkpoints commit independently (at-least-once).
- Do not call `start`/`resume`/`cancel` inside `Repo.transaction` on the Interruptus-configured repo.
- `lock_version` fences workflow-row updates only — not host-table writes from a stale runner.
- Use `Interruptus.Effect` markers + `verify/1` for DB side effects; domain uniqueness still required for true safety.
- For pool isolation: `{Interruptus, repo: MyApp.InterruptusRepo}` while stages use `MyApp.Repo`.

## Authoring Rules

### Define a workflow

```elixir
defmodule MyApp.TransferFunds do
  use Interruptus.Workflow

  workflow do
    param :from_account_id, :integer
    param :to_account_id, :integer
    param :amount, :decimal

    data :debit_ref, :string
    data :credit_ref, :string

    stage_timeout 30_000

    pipeline :validate_accounts

    checkpoint compensate: :reverse_debit do
      verify :verify_debit_applied/1
      pipeline :debit_account
    end

    checkpoint compensate: :reverse_credit do
      verify :verify_credit_applied/1
      pipeline :credit_account
    end

    pipeline :send_receipt

    restart_policy max_attempts: 5, backoff: :exponential
  end
end
```

Per-checkpoint `compensate:` is preferred — rollback runs LIFO over checkpoints
the workflow actually passed. `rollback_policy compensate: [...]` still works as
a workflow-level list appended after the per-checkpoint compensations.

### Stage return values

- Return the command struct for normal progress.
- `{:suspend, reason, metadata}` — voluntary suspension; no process held until resume.
- `halt/1` — stop forward progress; triggers restart or rollback per policy.
- Raised exceptions, throws, exits, invalid returns, and `stage_timeout` expiry
  are contained and routed through the restart policy (no crash loops).

### Typed fields

- `param :name, :type` and `data :name, :type` declare Ecto-typed fields.
- Params are cast and validated at `Interruptus.start/3` (required unless `default:` is set).
- Data is validated on persist (dump-then-cast); unset (`nil`) fields are omitted from JSONB.
- `:decimal` persists as a normalized string; use `Decimal` in stage functions after load/cast.
- Supports `Ecto.Enum` and custom `Ecto.Type` modules via field options.

### Verify functions

Each checkpoint segment may define a `verify/1` function:

- `:done` — external work already applied; skip segment stages.
- `:not_done` — re-run segment stages (at-least-once).
- `:failed` — unrecoverable; apply restart or rollback policy.

Verify functions **must be idempotent** and must not create duplicate side effects.

### Policies

- **restart_policy** — `max_attempts`, `backoff` (`:constant`, `:exponential`), optional `retryable_errors`. Attempts are persisted **before** execution and reset at each checkpoint, so budgets hold across crashes.
- **rollback_policy** — workflow-level `compensate: [...]` list appended after per-checkpoint compensations; invoked LIFO on terminal failure. Compensation progress is persisted per step (`compensation_index`) and crash-resumable.

## Invariants

1. **At-least-once** between checkpoints — stages and verify may run more than once.
2. **JSON-serializable state** — `params` and `data` must be JSON-encodable maps in v1.
3. **Fenced writes everywhere** — every state-changing write bumps `lock_version`; runner writes additionally require `locked_by = node AND locked_until > now()`. Renewal extends the lease without a bump. Fences workflow rows only.
4. **Terminal workflows are immutable** — `:completed`, `:compensated`, `:cancelled` are never restarted.
5. **One active runner** — cluster-wide exclusivity via PostgreSQL row claim (`SKIP LOCKED`) + heartbeat; heartbeats renew concurrently with stage execution.
6. **Initial checkpoint on start** — every workflow persists a snapshot at initiation.
7. **No nested API transactions** — `start`/`resume`/`cancel`/`Claim.acquire` reject an open transaction on the configured repo.
8. **Attempts persisted pre-execution** — crash loops are bounded by `max_attempts` and end in rollback (or `:failed` when nothing is compensable).
9. **Suspension requires explicit resume** — Recovery reclaims `:pending`/`:running`/`:compensating` only; `resume/2` transitions `:suspended → :pending` and `:failed → :compensating` with a fenced write.
10. **Version-checked claims** — a `pipeline_version` mismatch parks the workflow as `:suspended` (`"pipeline_version_mismatch"`); unresolvable `workflow_type` rows are skipped by recovery, never mutated.

## Conventions

- Workflow DSL: `param/3`, `data/3`, `pipeline`, `checkpoint` (with `compensate:`), `stage_timeout/1`, `halt/1`, `put_data/3`, `put_error/3`.
- Mirror **Oban** embedding: `Interruptus.Migration.up/0`, `Interruptus.Repo` wrapper.
- Module layout under `lib/interruptus/`.
- Public API on `Interruptus` module: `start/3`, `resume/2`, `cancel/2` (supports `compensate: true`), `status/2`. Start is idempotent per `idempotency_key`.
- Per-instance OTP tree started by `{Interruptus, repo: ...}` in the **host** supervisor; process names derive from config `:name`.
- Shared-DB effects: `Interruptus.Effect` markers + idempotent `verify/1`.
- Use `:telemetry` for observability events.

## Implementation Phases

| Phase | Scope |
|-------|-------|
| A | Mix scaffold, Migration, schemas, Repo, Config |
| B | Workflow DSL, Command helpers, in-memory Engine |
| C | Store, Claim, Recovery, public start/resume/cancel API |
| D | Runner GenServer, supervisors, registry, checkpoint loop |
| E | Restart/rollback policies, stage timeouts |
| F | Telemetry, Interruptus.Test, example app, docs |

## Testing Expectations

- Unit tests for DSL expansion and pure `Interruptus.Engine` execution.
- Integration tests with Ecto SQL Sandbox against PostgreSQL.
- Claim exclusivity tests (two competing claim attempts).
- Crash/resume tests via `Process.exit/2` and supervisor restart.
- Use `Interruptus.Test` helpers for simulating interrupts and asserting checkpoint state.

## Out of Scope (v1)

- Child workflows, cron triggers, visual UI.
- Non-PostgreSQL adapters (designed for future extension).
- Distributed tracing beyond `:telemetry`.
- Retention/GC plugin (configurable purge deferred).
