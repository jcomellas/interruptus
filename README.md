# Interruptus

Durable Commandex-style workflow pipelines for Elixir with checkpoint-based persistence, multi-node exclusivity, and explicit suspend/resume.

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

# application.ex
children = [
  MyApp.Repo,
  {Interruptus, repo: MyApp.Repo}
]
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
    param :from_account_id
    param :amount

    data :debit_ref

    pipeline :validate

    checkpoint do
      verify :verify_debit/1
      pipeline :debit_account
    end

    restart_policy max_attempts: 3, backoff: :exponential
    rollback_policy compensate: [:reverse_debit]
  end
end

{:ok, workflow} = Interruptus.start(MyApp.TransferFunds, %{from_account_id: "a", amount: 100})
```

See [DESIGN.md](DESIGN.md) for architecture details and [AGENTS.md](AGENTS.md) for contributor context.

## License

MIT
