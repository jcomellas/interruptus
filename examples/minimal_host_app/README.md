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

## Start a workflow

```elixir
iex -S mix

{:ok, workflow} =
  Interruptus.start(MinimalHostApp.Workflows.TransferFunds, %{
    from_account_id: "acct-1",
    to_account_id: "acct-2",
    amount: 100
  })

Interruptus.status(workflow.id)
```
