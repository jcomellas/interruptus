# Minimal Host App

Example OTP application demonstrating Interruptus integration.

## Setup

```bash
cd examples/minimal_host_app
mix deps.get
mix ecto.create
mix ecto.migrate
```

Add migration:

```elixir
defmodule MinimalHostApp.Repo.Migrations.AddInterruptus do
  use Ecto.Migration
  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down()
end
```

## Supervision (shared or dedicated pool)

```elixir
# Shared host Repo (simple)
children = [
  MinimalHostApp.Repo,
  {Interruptus, repo: MinimalHostApp.Repo}
]

# Dedicated Interruptus pool (same DB, separate connections) under load:
# children = [
#   MinimalHostApp.Repo,
#   MinimalHostApp.InterruptusRepo,
#   {Interruptus, repo: MinimalHostApp.InterruptusRepo}
# ]
```

Do not call `Interruptus.start/3`, `resume/2`, or `cancel/2` inside
`Repo.transaction/2`. Nesting is rejected on the Interruptus-configured repo
with `{:error, :in_transaction}`.

## Start a workflow

```elixir
iex -S mix

{:ok, workflow} =
  Interruptus.start(MinimalHostApp.Workflows.TransferFunds, %{
    from_account_id: 1,
    to_account_id: 2,
    amount: "100.00"
  })

Interruptus.status(workflow.id)
```

The transfer example uses `Interruptus.Effect.once/4` and `Effect.exists?/3` in
verify so debit/credit markers survive at-least-once replay between checkpoints.
