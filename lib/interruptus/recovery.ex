defmodule Interruptus.Recovery do
  @moduledoc """
  Scans for reclaimable workflows on boot and periodically thereafter.
  """

  use GenServer

  alias Interruptus.Config
  alias Interruptus.Store

  @doc false
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    config = Keyword.get(opts, :config, Config.fetch())
    schedule_recovery(config.recovery_interval)
    {:ok, %{config: config}}
  end

  @impl true
  def handle_info(:recover, %{config: config} = state) do
    recover_all(config)
    schedule_recovery(config.recovery_interval)
    {:noreply, state}
  end

  @doc """
  Recovers all reclaimable workflows for the given config.
  """
  @spec recover_all(Config.t()) :: :ok
  def recover_all(config) do
    config
    |> Store.list_reclaimable()
    |> Enum.each(fn instance ->
      workflow_module = module_from_type(instance.workflow_type)
      Interruptus.RunnerSupervisor.start_runner(config, workflow_module, instance.id)
    end)

    :ok
  end

  defp schedule_recovery(interval) do
    Process.send_after(self(), :recover, interval)
  end

  defp module_from_type(type) when is_binary(type) do
    type
    |> String.split(".")
    |> Module.concat()
  end
end
