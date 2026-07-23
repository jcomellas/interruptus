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
  exits, timeouts, and `halt/2` all flow through the restart policy.
- **Suspension is explicit** — a `:suspended` workflow is never auto-resumed by
  recovery. `Interruptus.resume/2` performs a fenced `suspended → pending`
  transition. A `:failed` workflow resumes into `:compensating` to retry rollback.
- **Compensation is durable** — `compensation_index` is persisted after each
  compensation function; a crash mid-rollback is reclaimed and resumes from the
  last completed step. Compensations run only for checkpoints that were passed.
- **Cancel fences live runners** — `Interruptus.cancel/2` bumps the fencing
  token; a running runner (even with a valid lease) fails its next write and
  stops. `cancel(id, compensate: true)` rolls back passed checkpoints instead
  (ends `:compensated`).
- **Long stages are safe** — lease heartbeats renew concurrently with stage
  execution, so a stage longer than `lease_duration` does not lose exclusivity.
- **Deploy skew is detected** — a persisted `pipeline_version` that differs from
  the compiled workflow parks the instance as `:suspended`
  (`"pipeline_version_mismatch"`) instead of misexecuting positional indexes.

## Durability and the database

Interruptus persists workflow state in the host application's PostgreSQL database (Oban-style embedding). Stages run **outside** Interruptus transactions:

- Stage DB writes and checkpoint writes are **independent commits** — not one atomic unit.
- Segments between checkpoints may run **at-least-once** after a crash; use idempotent side effects, domain unique constraints, and checkpoint `verify/1`.
- `Interruptus.Effect` records `(workflow_id, effect_key)` markers so successful work can be skipped on replay. Markers are not written for halted or suspended results, and they do not defend against two runners racing in a lease-expiry window — use domain unique constraints for hard once-only guarantees.
- `lock_version` is a fencing token bumped on every state-changing write. It fences **workflow-row** updates only — not host-table writes from a stale runner after lease expiry.

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
