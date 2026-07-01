defmodule Interruptus do
  @moduledoc """
  Durable Commandex-style workflow pipelines for Elixir.

  Interruptus runs workflow pipelines on the BEAM with checkpoint-based durability,
  cluster-wide exclusivity via PostgreSQL leases, and explicit suspend/resume. Workflows
  survive process crashes and application restarts without an external orchestrator.

  ## Setup

  Add Interruptus to your host application supervisor:

      children = [
        MyApp.Repo,
        {Interruptus, repo: MyApp.Repo}
      ]

  Run `Interruptus.Migration.up/0` from your Ecto migrations, then define workflows
  with `Interruptus.Workflow` and start them via `start/3`.

  ## Lifecycle

  1. `start/3` inserts a `:pending` row and starts a `Interruptus.Runner`.
  2. The runner claims the row, executes stages, and writes checkpoints.
  3. On suspend, state is persisted and the runner stops until `resume/2`.
  4. On crash, `Interruptus.Recovery` reclaims expired leases and restarts runners.
  5. Terminal workflows (`:completed`, `:compensated`, `:cancelled`) are never restarted.

  See `Interruptus.Workflow` for defining workflows and `DESIGN.md` for architecture.
  """

  alias Interruptus.Config
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  @doc """
  Returns a child spec for the host application supervisor.

  Merges `opts` into `Interruptus.Config` and stores it via `Interruptus.Config.put/1`
  when the child starts.

  ## Arguments

    * `opts` - keyword list of config overrides (see `Interruptus.Config`)

  ## Options

    * `:repo` - required Ecto repo module (e.g. `MyApp.Repo`)
    * `:name` - config name atom (default `Interruptus`)
    * `:prefix` - PostgreSQL schema prefix (default `"public"`)
    * `:lease_duration` - lease TTL in ms (default `30_000`)
    * `:heartbeat_interval` - lease renewal interval in ms (default `10_000`)
    * `:recovery_interval` - stale-workflow scan interval in ms (default `5_000`)

  ## Returns

    * A supervisor child spec map suitable for `Supervisor.start_link/2`

  ## Examples

      {Interruptus, repo: MyApp.Repo}
  """
  def child_spec(opts) do
    config = Config.new(opts) |> Config.put()

    %{
      id: config.name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  # Starts Interruptus and runs an initial recovery scan.
  # Called by the supervisor via child_spec/1. Returns :ignore.
  @doc false
  def start_link(opts) do
    config = Config.new(opts) |> Config.put()
    Interruptus.Recovery.recover_all(config)
    :ignore
  end

  @doc """
  Starts a new workflow instance and its runner process.

  Inserts a `:pending` row (with an initial checkpoint), then starts a
  `Interruptus.Runner` under `Interruptus.RunnerSupervisor`.

  ## Arguments

    * `workflow_module` - module using `Interruptus.Workflow`
    * `params` - map or keyword list of workflow parameters (JSON-serializable values)

  ## Options

    * `:idempotency_key` - optional unique key per workflow type; duplicate keys
      cause insert failure via the database unique index
    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - instance row with generated `id`
    * `{:error, %Ecto.Changeset{}}` - validation or constraint failure on insert
    * `{:error, term()}` - runner could not be started

  ## Examples

      {:ok, instance} = Interruptus.start(MyApp.TransferFunds, %{amount: 100})
      {:ok, instance} = Interruptus.start(MyApp.TransferFunds, amount: 100, idempotency_key: "tx-1")
  """
  @spec start(module(), map() | keyword(), keyword()) ::
          {:ok, WorkflowInstance.t()} | {:error, term()}
  def start(workflow_module, params, opts \\ []) do
    config = config_from_opts(opts)

    attrs = %{
      workflow_type: workflow_module |> Module.split() |> Enum.join("."),
      status: :pending,
      params: normalize_map(params),
      data: %{},
      current_stage_index: 0,
      pipeline_version: workflow_module.pipeline_version(),
      idempotency_key: Keyword.get(opts, :idempotency_key)
    }

    with {:ok, instance} <- Store.insert_workflow(config, attrs),
         {:ok, _pid} <- RunnerSupervisor.start_runner(config, workflow_module, instance.id) do
      :telemetry.execute(
        [:interruptus, :workflow, :started],
        %{},
        %{workflow_id: instance.id, workflow_type: instance.workflow_type}
      )

      {:ok, instance}
    end
  end

  @doc """
  Resumes a suspended or reclaimable workflow by starting a new runner.

  Looks up the instance, verifies it is not terminal, resolves the workflow module
  from `workflow_type`, and starts a runner. If a runner is already registered for
  the workflow id, `Interruptus.RunnerSupervisor` returns the existing pid.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, pid()}` - runner process pid (new or existing)
    * `{:error, :not_found}` - no row with that id
    * `{:error, :terminal}` - workflow is `:completed`, `:compensated`, or `:cancelled`
    * `{:error, term()}` - runner could not be started
  """
  @spec resume(Ecto.UUID.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def resume(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    with %WorkflowInstance{} = instance <- Store.get(config, workflow_id),
         false <- WorkflowInstance.terminal?(instance),
         workflow_module <- module_from_type(instance.workflow_type),
         {:ok, pid} <- RunnerSupervisor.start_runner(config, workflow_module, workflow_id) do
      :telemetry.execute(
        [:interruptus, :workflow, :resumed],
        %{},
        %{workflow_id: workflow_id}
      )

      {:ok, pid}
    else
      nil -> {:error, :not_found}
      true -> {:error, :terminal}
      error -> error
    end
  end

  @doc """
  Cancels a non-terminal workflow.

  Sets status to `:cancelled` and clears the lease (`locked_by`, `locked_until`).
  Requires a successful optimistic lock on `lock_version`.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - updated instance with `:cancelled` status
    * `{:error, :not_found}` - no row with that id
    * `{:error, :terminal}` - workflow is already terminal
    * `{:error, :stale_lock}` - another runner holds a newer `lock_version`
  """
  @spec cancel(Ecto.UUID.t(), keyword()) :: {:ok, WorkflowInstance.t()} | {:error, term()}
  def cancel(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    with %WorkflowInstance{} = instance <- Store.get(config, workflow_id),
         false <- WorkflowInstance.terminal?(instance),
         {:ok, cancelled} <-
           Store.update_with_lock(config, instance, %{
             status: :cancelled,
             locked_by: nil,
             locked_until: nil
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :cancelled],
        %{},
        %{workflow_id: workflow_id}
      )

      {:ok, cancelled}
    else
      nil -> {:error, :not_found}
      true -> {:error, :terminal}
      error -> error
    end
  end

  @doc """
  Returns the current persisted state of a workflow instance.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - current row including `status`, `params`, `data`,
      `current_stage_index`, lease fields, and errors
    * `{:error, :not_found}` - no row with that id
  """
  @spec status(Ecto.UUID.t(), keyword()) :: {:ok, WorkflowInstance.t()} | {:error, :not_found}
  def status(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    case Store.get(config, workflow_id) do
      nil -> {:error, :not_found}
      instance -> {:ok, instance}
    end
  end

  defp config_from_opts(opts) do
    opts
    |> Keyword.get(:config, Interruptus)
    |> Config.fetch()
  end

  defp module_from_type(type) do
    type |> String.split(".") |> Module.concat()
  end

  defp normalize_map(params) when is_list(params), do: params |> Map.new() |> normalize_map()

  defp normalize_map(params) when is_map(params) do
    Map.new(params, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end
end
