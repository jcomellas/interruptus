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

  Workflow rows whose `workflow_type` cannot be resolved are parked as
  `:suspended` with reason `"unknown_workflow_type"` so they leave the reclaim
  set and cannot starve newer reclaimable work. An operator (or a node that
  loads the module) can resume or cancel them explicitly.

  When `purge_schedule: true` and `retention_ms` are configured, each scan also
  deletes terminal workflows older than the retention window via
  `Interruptus.purge_terminal/1` (off by default).
  """

  use GenServer

  require Logger

  alias Interruptus.Config
  alias Interruptus.Store
  alias Interruptus.WorkflowType

  @typep state :: %{config: Config.t()}

  @page_size 100
  # Bound work per scan so a huge backlog cannot block the GenServer forever.
  @max_pages 50

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
    maybe_purge_terminal(config)
    schedule_recovery(jittered_interval(config))
    {:noreply, state}
  end

  @doc """
  Recovers reclaimable workflows for the given config.

  Pages through reclaimable rows with keyset pagination until a page is empty
  or the per-scan page budget is exhausted. Safe to call concurrently;
  duplicate runners are prevented by the registry check in `RunnerSupervisor`
  and by lease claiming.

  ## Arguments

    * `config` - Interruptus config

  ## Returns

    * `:ok`
  """
  @spec recover_all(Config.t()) :: :ok
  def recover_all(config) do
    now = DateTime.utc_now()
    recover_pages(config, now, nil, 0)
    :ok
  end

  @spec recover_pages(Config.t(), DateTime.t(), {DateTime.t(), Ecto.UUID.t()} | nil, non_neg_integer()) ::
          :ok
  defp recover_pages(_config, _now, _cursor, page) when page >= @max_pages, do: :ok

  defp recover_pages(config, now, cursor, page) do
    opts =
      case cursor do
        nil -> [limit: @page_size]
        after_cursor -> [limit: @page_size, after: after_cursor]
      end

    case Store.list_reclaimable(config, now, opts) do
      [] ->
        :ok

      batch ->
        Enum.each(batch, &recover_instance(config, &1))
        last = List.last(batch)
        recover_pages(config, now, {last.inserted_at, last.id}, page + 1)
    end
  end

  @spec recover_instance(Config.t(), Interruptus.Schemas.WorkflowInstance.t()) :: :ok
  defp recover_instance(config, instance) do
    case WorkflowType.resolve(instance.workflow_type) do
      {:ok, workflow_module} ->
        _ = Interruptus.RunnerSupervisor.start_runner(config, workflow_module, instance.id)
        :ok

      {:error, :unknown_workflow_type} ->
        park_unknown_workflow_type(config, instance)
    end
  end

  @spec park_unknown_workflow_type(Config.t(), Interruptus.Schemas.WorkflowInstance.t()) :: :ok
  defp park_unknown_workflow_type(config, instance) do
    Logger.warning(
      "interruptus recovery parking workflow_id=#{instance.id} " <>
        "workflow_type=#{instance.workflow_type}: module not resolvable on this node"
    )

    :telemetry.execute(
      [:interruptus, :recovery, :unknown_workflow_type],
      %{},
      %{workflow_id: instance.id, workflow_type: instance.workflow_type}
    )

    _ =
      Store.update_with_lock(config, instance, %{
        status: :suspended,
        suspend_reason: "unknown_workflow_type",
        locked_by: nil,
        locked_until: nil
      })

    :ok
  end

  @spec maybe_purge_terminal(Config.t()) :: :ok
  defp maybe_purge_terminal(%Config{purge_schedule: true, retention_ms: ms} = config)
       when is_integer(ms) and ms > 0 do
    _ = Interruptus.purge_terminal(config: config.name, older_than: ms)
    :ok
  end

  defp maybe_purge_terminal(_config), do: :ok

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
