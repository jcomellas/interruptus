defmodule Interruptus.Application do
  @moduledoc """
  OTP application callback for the `:interruptus` application.

  Starts the internal supervision tree when Interruptus is listed as a
  dependency application (library mode). Host applications typically start
  Interruptus via `{Interruptus, repo: MyApp.Repo}` instead.

  ## Supervision tree

    * `Interruptus.Registry` — workflow_id → runner pid
    * `Interruptus.RunnerSupervisor` — DynamicSupervisor for runners
    * `Interruptus.Recovery` — periodic stale-workflow reclaim
  """

  use Application

  # Application start callback. Starts Interruptus.Supervisor with :one_for_one strategy.
  @doc false
  @spec start(Application.start_type(), [term()]) :: Supervisor.on_start()
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
