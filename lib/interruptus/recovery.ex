defmodule Interruptus.Recovery do
  @moduledoc """
  Scans for reclaimable workflows on boot and periodically thereafter.

  Workflows with expired leases (crashed runners, network partitions) are
  restarted by calling `Interruptus.RunnerSupervisor.start_runner/3`. Also
  invoked synchronously when the Interruptus child starts on application boot.
  """

  use GenServer

  alias Interruptus.Config
  alias Interruptus.Store

  @typep state :: %{config: Config.t()}

  # Starts the Recovery GenServer named Interruptus.Recovery.
  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # GenServer init callback. Schedules the first recovery scan.
  @doc false
  @spec init(keyword()) :: {:ok, state()}
  @impl true
  def init(opts) do
    config = Keyword.get(opts, :config, Config.fetch())

    if Keyword.get(opts, :schedule, recovery_schedule_enabled?()) do
      schedule_recovery(config.recovery_interval)
    end

    {:ok, %{config: config}}
  end

  @spec recovery_schedule_enabled?() :: boolean()
  defp recovery_schedule_enabled? do
    Application.get_env(:interruptus, :recovery_schedule, true)
  end

  # Handles :recover — runs recover_all/1 and reschedules the next scan.
  @doc false
  @spec handle_info(:recover, state()) :: {:noreply, state()}
  @impl true
  def handle_info(:recover, %{config: config} = state) do
    recover_all(config)
    schedule_recovery(config.recovery_interval)
    {:noreply, state}
  end

  @doc """
  Recovers all reclaimable workflows for the given config.

  Queries `Interruptus.Store.list_reclaimable/1` and starts a runner for each
  instance. Safe to call concurrently; duplicate runners are prevented by the
  registry check in `RunnerSupervisor`.

  ## Arguments

    * `config` - Interruptus config

  ## Returns

    * `:ok`
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

  @spec schedule_recovery(pos_integer()) :: reference()
  defp schedule_recovery(interval) do
    Process.send_after(self(), :recover, interval)
  end

  @spec module_from_type(String.t()) :: module()
  defp module_from_type(type) when is_binary(type) do
    type
    |> String.split(".")
    |> Module.concat()
  end
end
