defmodule Interruptus.RunnerSupervisor do
  @moduledoc """
  DynamicSupervisor for per-workflow Runner processes.
  """

  use DynamicSupervisor

  alias Interruptus.Config

  @doc false
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts a Runner for the given workflow instance.
  """
  @spec start_runner(Config.t(), module(), Ecto.UUID.t()) ::
          {:ok, pid()} | {:error, term()}
  def start_runner(config, workflow_module, workflow_id) do
    case Registry.lookup(Interruptus.Registry, workflow_id) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        spec =
          {Interruptus.Runner,
           config: config, workflow_module: workflow_module, workflow_id: workflow_id}

        DynamicSupervisor.start_child(__MODULE__, spec)
    end
  end
end
