defmodule Interruptus.Test.Support.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      alias Interruptus.Config
      alias Interruptus.Test.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
    end
  end

  setup tags do
    :ok = Interruptus.Test.Support.Runtime.start!()

    pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Interruptus.Test.Repo, shared: not tags[:async])

    on_exit(fn -> Ecto.Adapters.SQL.Sandbox.stop_owner(pid) end)

    unless tags[:async] do
      on_exit(fn -> Interruptus.Test.Support.Runtime.cleanup!() end)
    end

    unless tags[:async] do
      Ecto.Adapters.SQL.Sandbox.mode(Interruptus.Test.Repo, {:shared, pid})
    end

    config =
      Interruptus.Config.new(
        repo: Interruptus.Test.Repo,
        node_id: "test-node-#{System.unique_integer()}"
      )
      |> Interruptus.Config.put()

    {:ok, config: config, sandbox_owner: pid}
  end
end
