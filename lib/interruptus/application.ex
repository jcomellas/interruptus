defmodule Interruptus.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Interruptus.Registry},
      Interruptus.RunnerSupervisor,
      Interruptus.Recovery
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Interruptus.Supervisor)
  end
end
