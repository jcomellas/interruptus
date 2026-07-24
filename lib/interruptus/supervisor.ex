defmodule Interruptus.Supervisor do
  @moduledoc """
  Per-instance supervision tree for Interruptus.

  Started by the host application via `{Interruptus, repo: MyApp.Repo}`, which
  guarantees the tree boots **after** the host Repo. Each Interruptus instance
  (distinguished by `:name`) runs its own tree, so multiple instances can
  coexist in one VM (e.g. umbrella apps or a dedicated-pool instance):

      Interruptus.Supervisor            (Module.concat(name, Supervisor))
      ├── Registry                      (Module.concat(name, Registry))
      ├── Task.Supervisor               (Module.concat(name, TaskSupervisor))
      ├── Interruptus.RunnerSupervisor  (Module.concat(name, RunnerSupervisor))
      └── Interruptus.Recovery          (Module.concat(name, Recovery))

  The Registry maps workflow ids to runner pids. The Task.Supervisor executes
  workflow stages so runner GenServers stay responsive for lease heartbeats.
  Recovery receives its config at start and never reads global state at boot.
  """

  use Supervisor

  alias Interruptus.Config

  @doc """
  Starts the Interruptus instance supervision tree.

  Builds an `Interruptus.Config` from `opts` merged over application env,
  stores it in `:persistent_term`, and starts the per-instance children.

  ## Arguments

    * `opts` - keyword list of config overrides (see `Interruptus.Config`)

  ## Returns

    * `{:ok, pid}` - supervisor started
    * `{:error, term()}` - startup failure
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    config = Config.new(opts) |> Config.put()
    Supervisor.start_link(__MODULE__, config, name: Config.supervisor_name(config))
  end

  @doc false
  @impl true
  @spec init(Config.t()) :: {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  def init(%Config{} = config) do
    children = [
      {Registry, keys: :unique, name: Config.registry_name(config)},
      {Task.Supervisor, name: Config.task_supervisor_name(config)},
      {Interruptus.RunnerSupervisor, name: Config.runner_supervisor_name(config)},
      {Interruptus.Recovery, config: config, name: Config.recovery_name(config)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
