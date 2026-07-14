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
config :my_app, Interruptus,
  repo: MyApp.Repo,
  prefix: "public",
  node_id: "node-1",
  lease_duration: 30_000,
  heartbeat_interval: 10_000

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

    pipeline :validate

    checkpoint do
      verify :verify_debit
      pipeline :debit_account
    end

    restart_policy max_attempts: 3, backoff: :exponential
    rollback_policy compensate: [:reverse_debit]
  end
end

{:ok, workflow} = Interruptus.start(MyApp.TransferFunds, %{from_account_id: 1, amount: "100.00"})
```

## Durability and the database

Interruptus persists workflow state in the host application's PostgreSQL database (Oban-style embedding). Stages run **outside** Interruptus transactions:

- Stage DB writes and checkpoint writes are **independent commits** — not one atomic unit.
- Segments between checkpoints may run **at-least-once** after a crash; use idempotent side effects, domain unique constraints, and checkpoint `verify/1`.
- `Interruptus.Effect` records `(workflow_id, effect_key)` markers so successful work can be skipped on replay.
- `lock_version` fences **workflow-row** updates only — not host-table writes from a stale runner after lease expiry.

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
