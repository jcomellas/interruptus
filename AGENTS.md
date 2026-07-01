# Interruptus — Agent Context

## Mission

Interruptus is an Elixir library that provides **Commandex-style workflow pipelines** with **checkpoint-based durability**, **multi-node exclusivity**, and **explicit suspend/resume**. It is a simpler, in-process alternative to Temporal: workflows run on the BEAM, persist state in the host application's PostgreSQL database, and resume after crashes, restarts, or voluntary suspension without requiring an external orchestrator.

## Architecture Map

```
Host Application
├── {Interruptus, repo: MyApp.Repo}     # OTP child under application supervisor
└── MyApp.Repo                        # Host-owned Ecto repo

Interruptus OTP Tree
├── Interruptus.Supervisor
│   ├── Interruptus.Registry          # workflow_id → Runner pid
│   ├── Interruptus.Recovery          # Boot scan + stale lease reclaim
│   └── Interruptus.RunnerSupervisor  # DynamicSupervisor for runners
└── Interruptus.Runner (per instance) # GenServer executing pipeline segments

Database (embedded via Interruptus.Migration)
├── interruptus_workflows             # Instance rows, locks, snapshots
├── interruptus_checkpoints           # Historical checkpoint audit trail
└── interruptus_stage_attempts        # Per-stage execution log
```

### Claim / Recover Loop

1. `Interruptus.start/2` inserts a `:pending` workflow row and starts a Runner.
2. Runner claims the row (`locked_by`, `locked_until`, `lock_version`) in a transaction.
3. Runner executes segments: verify → stages → checkpoint → advance.
4. On suspend: persist snapshot, set `:suspended`, release lease, stop process.
5. On crash: lease expires; `Interruptus.Recovery` reclaims and starts a new Runner.
6. On complete: set `:completed`; Recovery never restarts terminal workflows.

## Authoring Rules

### Define a workflow

```elixir
defmodule MyApp.TransferFunds do
  use Interruptus.Workflow

  workflow do
    param :from_account_id
    param :to_account_id
    param :amount

    data :debit_ref
    data :credit_ref

    pipeline :validate_accounts

    checkpoint do
      verify :verify_debit_applied/1
      pipeline :debit_account
    end

    checkpoint do
      verify :verify_credit_applied/1
      pipeline :credit_account
    end

    pipeline :send_receipt

    restart_policy max_attempts: 5, backoff: :exponential
    rollback_policy compensate: [:reverse_debit, :reverse_credit]
  end
end
```

### Stage return values

- Return the command struct (Commandex-compatible) for normal progress.
- `{:suspend, reason, metadata}` — voluntary suspension; no process held until resume.
- `halt/1` — stop forward progress; triggers restart or rollback per policy.

### Verify functions

Each checkpoint segment may define a `verify/1` function:

- `:done` — external work already applied; skip segment stages.
- `:not_done` — re-run segment stages (at-least-once).
- `:failed` — unrecoverable; apply restart or rollback policy.

Verify functions **must be idempotent** and must not create duplicate side effects.

### Policies

- **restart_policy** — `max_attempts`, `backoff` (`:constant`, `:exponential`), optional `retryable_errors`.
- **rollback_policy** — `compensate: [...]` list of functions invoked LIFO on terminal failure.

## Invariants

1. **At-least-once** between checkpoints — stages and verify may run more than once.
2. **JSON-serializable state** — `params` and `data` must be JSON-encodable maps in v1.
3. **Lease required for writes** — checkpoint/status updates require valid `lock_version`.
4. **Terminal workflows are immutable** — `:completed`, `:compensated`, `:cancelled` are never restarted.
5. **One active runner** — cluster-wide exclusivity via PostgreSQL row claim + heartbeat.
6. **Initial checkpoint on start** — every workflow persists a snapshot at initiation.

## Conventions

- Mirror **Commandex** API: `param`, `data`, `pipeline`, `halt/1`, `put_data/3`, `put_error/3`.
- Mirror **Oban** embedding: `Interruptus.Migration.up/0`, `Interruptus.Repo` wrapper.
- Module layout under `lib/interruptus/`.
- Public API on `Interruptus` module: `start/2`, `resume/1`, `cancel/1`, `status/1`.
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
