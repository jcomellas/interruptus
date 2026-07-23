defmodule Interruptus.Recovery do
  @moduledoc """
  Scans for reclaimable workflows on boot and periodically thereafter.

  Workflows with expired leases (crashed runners, network partitions) in
  status `:pending`, `:running`, or `:compensating` are restarted by calling
  `Interruptus.RunnerSupervisor.start_runner/3`. Suspended workflows are
  **never** auto-resumed — they require an explicit `Interruptus.resume/2`.

  One Recovery process runs per Interruptus instance (see
  `Interruptus.Supervisor`) and receives its config at start, so multiple
  named instances are all recovered and no global state is read at boot.
  Scan scheduling adds a small random jitter so multiple nodes do not scan in
  lockstep.

  Workflow rows whose `workflow_type` cannot be resolved to a loaded workflow
  module (e.g. renamed module, or rolling deploy where this node runs older
  code) are skipped with a warning and a
  `[:interruptus, :recovery, :unknown_workflow_type]` telemetry event —
  never mutated — so a node running newer code can still pick them up.
  """

  use GenServer

  require Logger

  alias Interruptus.Config
  alias Interruptus.Store
  alias Interruptus.WorkflowType

  @typep state :: %{config: Config.t()}

  # Starts a Recovery GenServer. Options: :config (required), :name.
  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # GenServer init callback. Schedules the first recovery scan when enabled.
  @doc false
  @spec init(keyword()) :: {:ok, state()}
  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)

    if config.recovery_schedule do
      # First scan runs shortly after boot (with jitter) to reclaim workflows
      # orphaned by the previous shutdown or crash.
      schedule_recovery(initial_delay(config))
    end

    {:ok, %{config: config}}
  end

  # Handles :recover — runs recover_all/1 and reschedules the next scan.
  @doc false
  @spec handle_info(:recover, state()) :: {:noreply, state()}
  @impl true
  def handle_info(:recover, %{config: config} = state) do
    recover_all(config)
    schedule_recovery(jittered_interval(config))
    {:noreply, state}
  end

  @doc """
  Recovers all reclaimable workflows for the given config.

  Queries `Interruptus.Store.list_reclaimable/1` and starts a runner for each
  instance. Safe to call concurrently; duplicate runners are prevented by the
  registry check in `RunnerSupervisor` and by lease claiming.

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
      case WorkflowType.resolve(instance.workflow_type) do
        {:ok, workflow_module} ->
          Interruptus.RunnerSupervisor.start_runner(config, workflow_module, instance.id)

        {:error, :unknown_workflow_type} ->
          Logger.warning(
            "interruptus recovery skipped workflow_id=#{instance.id} " <>
              "workflow_type=#{instance.workflow_type}: module not resolvable on this node"
          )

          :telemetry.execute(
            [:interruptus, :recovery, :unknown_workflow_type],
            %{},
            %{workflow_id: instance.id, workflow_type: instance.workflow_type}
          )
      end
    end)

    :ok
  end

  @spec schedule_recovery(pos_integer()) :: reference()
  defp schedule_recovery(interval) do
    Process.send_after(self(), :recover, interval)
  end

  @spec initial_delay(Config.t()) :: pos_integer()
  defp initial_delay(%Config{recovery_interval: interval}) do
    :rand.uniform(interval)
  end

  @spec jittered_interval(Config.t()) :: pos_integer()
  defp jittered_interval(%Config{recovery_interval: interval}) do
    interval + :rand.uniform(max(div(interval, 5), 1))
  end
end
