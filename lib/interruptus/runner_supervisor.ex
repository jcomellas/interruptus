defmodule Interruptus.RunnerSupervisor do
  @moduledoc """
  DynamicSupervisor for per-workflow `Interruptus.Runner` processes.

  One instance runs per Interruptus config name (see `Interruptus.Supervisor`).
  Ensures at most one runner per workflow id on this node by checking the
  per-instance Registry before starting a child; a start race is resolved by
  the Runner's own Registry registration (the loser returns `:ignore`).
  """

  use DynamicSupervisor

  alias Interruptus.Config

  # Starts the DynamicSupervisor. Options: :name (per-instance process name).
  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  # DynamicSupervisor init callback. Uses :one_for_one strategy.
  @doc false
  @spec init(keyword()) :: {:ok, map()}
  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a Runner for the given workflow instance.

  If a runner is already registered for `workflow_id`, returns the existing pid
  without starting a duplicate.

  ## Arguments

    * `config` - Interruptus config
    * `workflow_module` - module using `Interruptus.Workflow`
    * `workflow_id` - UUID of the workflow instance

  ## Returns

    * `{:ok, pid()}` - new or existing runner pid
    * `{:error, term()}` - child could not be started
  """
  @spec start_runner(Config.t(), module(), Ecto.UUID.t()) ::
          {:ok, pid()} | {:error, term()}
  def start_runner(config, workflow_module, workflow_id) do
    registry = Config.registry_name(config)

    case Registry.lookup(registry, workflow_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        do_start_runner(config, workflow_module, workflow_id, registry)
    end
  end

  @doc """
  Evicts any registered runner for `workflow_id`, then starts a fresh one.

  Used after a fenced administrative transition (e.g. `cancel(compensate: true)`)
  so a still-registered, lease-fenced runner cannot block compensation by
  occupying the Registry slot.

  ## Arguments

    * `config` - Interruptus config
    * `workflow_module` - module using `Interruptus.Workflow`
    * `workflow_id` - UUID of the workflow instance

  ## Returns

    * `{:ok, pid()}` - new runner pid
    * `{:error, term()}` - child could not be started
  """
  @spec replace_runner(Config.t(), module(), Ecto.UUID.t()) ::
          {:ok, pid()} | {:error, term()}
  def replace_runner(config, workflow_module, workflow_id) do
    registry = Config.registry_name(config)
    runner_sup = Config.runner_supervisor_name(config)

    case Registry.lookup(registry, workflow_id) do
      [{pid, _}] ->
        _ = DynamicSupervisor.terminate_child(runner_sup, pid)
        await_registry_clear(registry, workflow_id, 50)
        do_start_runner(config, workflow_module, workflow_id, registry)

      [] ->
        do_start_runner(config, workflow_module, workflow_id, registry)
    end
  end

  @spec do_start_runner(Config.t(), module(), Ecto.UUID.t(), atom()) ::
          {:ok, pid()} | {:error, term()}
  defp do_start_runner(config, workflow_module, workflow_id, registry) do
    spec =
      {Interruptus.Runner, config: config, workflow_module: workflow_module, workflow_id: workflow_id}

    case DynamicSupervisor.start_child(Config.runner_supervisor_name(config), spec) do
      {:ok, pid} ->
        {:ok, pid}

      :ignore ->
        # Lost a registration race; the winner is (or was) in the Registry.
        case Registry.lookup(registry, workflow_id) do
          [{pid, _}] -> {:ok, pid}
          [] -> {:error, :already_finished}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec await_registry_clear(atom(), Ecto.UUID.t(), non_neg_integer()) :: :ok
  defp await_registry_clear(_registry, _workflow_id, 0), do: :ok

  defp await_registry_clear(registry, workflow_id, attempts) do
    case Registry.lookup(registry, workflow_id) do
      [] ->
        :ok

      [_ | _] ->
        Process.sleep(10)
        await_registry_clear(registry, workflow_id, attempts - 1)
    end
  end
end
