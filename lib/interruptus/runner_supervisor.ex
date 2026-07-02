defmodule Interruptus.RunnerSupervisor do
  @moduledoc """
  DynamicSupervisor for per-workflow `Interruptus.Runner` processes.

  Ensures at most one runner per workflow id by checking `Interruptus.Registry`
  before starting a child.
  """

  use DynamicSupervisor

  alias Interruptus.Config

  # Starts the DynamicSupervisor named Interruptus.RunnerSupervisor.
  @doc false
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # DynamicSupervisor init callback. Uses :one_for_one strategy.
  @doc false
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
    case Registry.lookup(Interruptus.Registry, workflow_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        spec =
          {Interruptus.Runner, config: config, workflow_module: workflow_module, workflow_id: workflow_id}

        DynamicSupervisor.start_child(__MODULE__, spec)
    end
  end
end
