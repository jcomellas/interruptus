# Interruptus

Durable workflow pipelines for Elixir with typed params/data, checkpoint-based persistence, multi-node exclusivity, and explicit suspend/resume.

## Installation

```elixir
def deps do
  [
    {:interruptus, "~> 0.1.0"}
  ]
end
```

## Quick Start

```elixir
# config/config.exs
config :interruptus, Interruptus,
  repo: MyApp.Repo,
  prefix: "public",
  lease_duration: 30_000,
  heartbeat_interval: 10_000,
  recovery_interval: 5_000

# :node_id defaults to "#{Node.self()}/<random-boot-token>", which is safe for
# multi-node deployments even without distributed Erlang. Set it explicitly if
# you want stable lease attribution across restarts.

# application.ex — shared repo (simple) or dedicated pool (recommended under load)
children = [
  MyApp.Repo,
  {Interruptus, repo: MyApp.Repo}
]

# Recommended for pool isolation (same Postgres database, separate pool):
# children = [
#   MyApp.Repo,
#   MyApp.InterruptusRepo,
#   {Interruptus, repo: MyApp.InterruptusRepo}
# ]
```

```elixir
# migration
defmodule MyApp.Repo.Migrations.AddInterruptus do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down()
end

# optional: isolate tables in a dedicated schema (match config :prefix)
defmodule MyApp.Repo.Migrations.AddPrefixedInterruptus do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up(prefix: "private")
  def down, do: Interruptus.Migration.down(prefix: "private")
end
```

```elixir
defmodule MyApp.TransferFunds do
  use Interruptus.Workflow

  workflow do
    param :from_account_id, :integer
    param :amount, :decimal

    data :debit_ref, :string
    data :credit_ref, :string

    stage_timeout 30_000

    pipeline :validate

    # Per-checkpoint compensations (preferred): run in LIFO order during
    # rollback, but only for checkpoints the workflow actually passed.
    checkpoint compensate: :reverse_debit do
      verify :verify_debit
      pipeline :debit_account
    end

    checkpoint compensate: :reverse_credit do
      verify :verify_credit
      pipeline :credit_account
    end

    restart_policy max_attempts: 3, backoff: :exponential
  end
end

# Idempotent start: retrying with the same key returns the existing instance.
{:ok, workflow} =
  Interruptus.start(MyApp.TransferFunds, %{from_account_id: 1, amount: "100.00"},
    idempotency_key: "transfer-1234"
  )
```

## Lifecycle semantics

- **Retries are bounded across crashes** — `attempt_count` is persisted *before*
  each execution attempt, so a crash-looping workflow ends in rollback after
  `max_attempts`, never in an infinite reclaim loop. Raised exceptions, throws,
  exits, timeouts, and `halt/1` all flow through the restart policy.
  `halt(command, success: true)` is a durable early exit to `:completed`
  (no compensation).
- **Suspension is explicit** — a `:suspended` workflow is never auto-resumed by
  recovery. `Interruptus.resume/2` performs a fenced `suspended → pending`
  transition. A `:failed` workflow with a non-empty compensation plan resumes
  into `:compensating`; an empty plan returns `{:error, :not_compensable}` and
  stays `:failed`.
- **Compensation is durable** — `compensation_index` is persisted after each
  compensation function; a crash mid-rollback is reclaimed and resumes from the
  last completed step. Compensations run for passed checkpoints (plus any
  workflow-level `rollback_policy` list). Entering compensation persists the
  current command snapshot so reclaim sees the same data.
- **Cancel fences live runners** — `Interruptus.cancel/2` bumps the fencing
  token; a running runner fails its next write and stops. `cancel(id, compensate: true)`
  fences the row, **evicts** any registered runner, and starts a fresh
  compensation runner (Recovery finishes the job if start fails).
- **Long stages and verify are safe** — lease heartbeats renew concurrently with
  segment tasks; `stage_timeout` applies to both stages and `verify/1`.
- **Start is durable first** — if the workflow row commits but the runner cannot
  start immediately, `start/3` still returns `{:ok, instance}`; Recovery reclaims
  lease-less `:pending` rows.
- **Deploy skew is detected** — a persisted `pipeline_version` that differs from
  the compiled workflow parks the instance as `:suspended`
  (`"pipeline_version_mismatch"`). Unresolvable `workflow_type` rows are parked
  as `:suspended` (`"unknown_workflow_type"`) so they cannot starve reclaim.
- **Suspend with mutations** — prefer `Command.suspend(command, reason, metadata)`
  (4-tuple) when the stage has already updated `data`/`params`; the 3-tuple
  `{:suspend, reason, metadata}` keeps the pre-stage command.

## Durability and the database

Interruptus persists workflow state in the host application's PostgreSQL database (Oban-style embedding). Stages run **outside** Interruptus transactions:

- Stage DB writes and checkpoint writes are **independent commits** — not one atomic unit.
- Segments between checkpoints may run **at-least-once** after a crash; use idempotent side effects, domain unique constraints, and checkpoint `verify/1`.
- `Interruptus.Effect` records `(workflow_id, effect_key)` markers so successful work can be skipped on replay. Markers are not written for halted or suspended results. A marker insert failure after a successful effect returns `{:error, {:effect_marker_failed, reason}}` so the restart policy applies (do not treat as success). Markers do not defend against two runners racing in a lease-expiry window — use domain unique constraints for hard once-only guarantees.
- `lock_version` is a fencing token bumped on every state-changing write. It fences **workflow-row** updates only — not host-table writes from a stale runner after lease expiry. In-flight external side effects may still complete after a fence until the process exits.

### Do not nest API calls in transactions

Do **not** call `Interruptus.start/3`, `resume/2`, or `cancel/2` inside `Repo.transaction/2` on the Interruptus-configured repo. Those calls return `{:error, :in_transaction}`. Nesting would turn Insert into a savepoint and start a Runner before the outer transaction commits.

Detection uses `config.repo.in_transaction?/0`. If Interruptus uses a dedicated `MyApp.InterruptusRepo`, wrapping `start` in `MyApp.Repo.transaction` is not detected — still do not nest start/resume/cancel in either repo's transaction.

### Dedicated repo pool

Sharing `MyApp.Repo` is supported. Under load, point Interruptus at a separate Repo module with its own pool size while stages keep using the application Repo:

```elixir
children = [
  MyApp.Repo,
  MyApp.InterruptusRepo,
  {Interruptus, repo: MyApp.InterruptusRepo}
]
```

See [DESIGN.md](DESIGN.md) for architecture details and [AGENTS.md](AGENTS.md) for contributor context.

## License

MIT
