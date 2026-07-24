defmodule Interruptus do
  @moduledoc """
  Durable workflow pipelines for Elixir with typed params and data.

  Interruptus runs workflow pipelines on the BEAM with checkpoint-based durability,
  cluster-wide exclusivity via PostgreSQL leases with fencing tokens, and explicit
  suspend/resume. Workflows survive process crashes and application restarts
  without an external orchestrator.

  ## Setup

  Add Interruptus to your host application supervisor, **after** the Repo:

      children = [
        MyApp.Repo,
        {Interruptus, repo: MyApp.Repo}
      ]

  This starts a per-instance supervision tree (`Interruptus.Supervisor`) with
  the runner Registry, a `Task.Supervisor` for stage execution, the runner
  DynamicSupervisor, and the Recovery scanner. Multiple named instances can
  coexist (`{Interruptus, repo: MyApp.OtherRepo, name: MyApp.Interruptus}`).

  For pool isolation under load, pass a dedicated Repo (same database URL, separate pool)
  as `:repo`; stage code can keep using `MyApp.Repo`.

  Run `Interruptus.Migration.up/0` from your Ecto migrations (pass the same
  `:prefix` as your config when using a non-public schema), then define workflows
  with `Interruptus.Workflow` and start them via `start/3`.

  ## Durability & transactions

  Stages execute **outside** Interruptus transactions. Side effects and checkpoints
  are separate commits (at-least-once between checkpoints). Use idempotent stage
  bodies, `verify/1`, and `Interruptus.Effect` markers for shared-DB work.

  Do not call `start/3`, `resume/2`, or `cancel/2` inside an open transaction on
  the Interruptus-configured repo (`{:error, :in_transaction}`).

  `lock_version` is a fencing token bumped by every state-changing write; it
  fences Interruptus workflow-row writes only — not host-table mutations from
  a stale runner.

  ## Lifecycle

  1. `start/3` inserts a `:pending` row and starts a `Interruptus.Runner`.
  2. The runner claims the row, executes stages in a supervised task (lease
     heartbeats continue during execution), and writes fenced checkpoints.
  3. On suspend, state is persisted and the runner stops. Only `resume/2`
     restarts a suspended workflow — Recovery never auto-resumes it.
  4. On crash, `Interruptus.Recovery` reclaims expired leases and restarts
     runners. Attempt budgets are persisted **before** execution, so crash
     loops are bounded by the workflow `restart_policy` and end in rollback.
  5. On failure after retries, compensation runs step-by-step with durable
     progress (`compensation_index`); a crash mid-compensation is reclaimed
     and resumed.
  6. Terminal workflows (`:completed`, `:compensated`, `:cancelled`) are never
     restarted. `:failed` workflows can be resumed to retry compensation.

  See `Interruptus.Workflow` for defining workflows and `DESIGN.md` for architecture.
  """

  alias Interruptus.Config
  alias Interruptus.Policy.Rollback
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store
  alias Interruptus.WorkflowType

  @cancel_retries 3

  @doc """
  Returns a child spec for the host application supervisor.

  Starts `Interruptus.Supervisor` (per-instance Registry, Task.Supervisor,
  RunnerSupervisor, and Recovery) with the merged `Interruptus.Config`.

  ## Arguments

    * `opts` - keyword list of config overrides (see `Interruptus.Config`)

  ## Options

    * `:repo` - required Ecto repo module (e.g. `MyApp.Repo`)
    * `:name` - config name atom (default `Interruptus`)
    * `:prefix` - PostgreSQL schema prefix (default `"public"`)
    * `:lease_duration` - lease TTL in ms (default `30_000`)
    * `:heartbeat_interval` - lease renewal interval in ms (default `10_000`)
    * `:recovery_interval` - stale-workflow scan interval in ms (default `5_000`)
    * `:recovery_schedule` - enable periodic recovery scans (default `true`)

  ## Returns

    * A supervisor child spec map suitable for `Supervisor.start_link/2`

  ## Examples

      {Interruptus, repo: MyApp.Repo}
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    name = Keyword.get(opts, :name, Interruptus)

    %{
      id: name,
      start: {Interruptus.Supervisor, :start_link, [opts]},
      type: :supervisor
    }
  end

  # Starts the per-instance supervision tree. Called via child_spec/1.
  @doc false
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Interruptus.Supervisor.start_link(opts)
  end

  @doc """
  Starts a new workflow instance and its runner process.

  Inserts a `:pending` row (with an initial checkpoint), then starts a
  `Interruptus.Runner` under `Interruptus.RunnerSupervisor`.

  When `:idempotency_key` is given and an instance with the same key and
  workflow type already exists, the **existing** instance is returned
  (`{:ok, instance}`), making `start/3` safe to retry.

  ## Arguments

    * `workflow_module` - module using `Interruptus.Workflow`
    * `params` - map or keyword list of workflow parameters (JSON-serializable values)

  ## Options

    * `:idempotency_key` - optional unique key per workflow type; duplicate
      keys return the existing instance
    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - new or existing instance row. Durable even
      when an immediate runner start fails; Recovery reclaims lease-less
      `:pending` rows.
    * `{:error, %Ecto.Changeset{}}` - validation or constraint failure on insert
    * `{:error, :in_transaction}` - called inside an open transaction on the
      Interruptus-configured repo (rejected to avoid nested-savepoint races)

  ## Examples

      {:ok, instance} = Interruptus.start(MyApp.TransferFunds, %{amount: 100})
      {:ok, instance} = Interruptus.start(MyApp.TransferFunds, amount: 100, idempotency_key: "tx-1")
  """
  @spec start(module(), map() | keyword(), keyword()) ::
          {:ok, WorkflowInstance.t()} | {:error, term()}
  def start(workflow_module, params, opts \\ []) do
    config = config_from_opts(opts)
    workflow_type = workflow_module |> Module.split() |> Enum.join(".")
    idempotency_key = Keyword.get(opts, :idempotency_key)

    with :ok <- reject_in_transaction(config),
         {:ok, cast_params} <- workflow_module.cast_params(params),
         {:ok, dumped_params} <- workflow_module.dump_params(cast_params),
         {:ok, dumped_data} <- workflow_module.dump_data(struct(workflow_module).data) do
      attrs = %{
        workflow_type: workflow_type,
        status: :pending,
        params: dumped_params,
        data: dumped_data,
        current_stage_index: 0,
        pipeline_version: workflow_module.pipeline_version(),
        pipeline_fingerprint: workflow_module.pipeline_fingerprint(),
        idempotency_key: idempotency_key
      }

      case insert_or_existing(config, workflow_type, idempotency_key, attrs) do
        {:ok, instance} ->
          case RunnerSupervisor.start_runner(config, workflow_module, instance.id) do
            {:ok, _pid} ->
              :telemetry.execute(
                [:interruptus, :workflow, :started],
                %{},
                %{workflow_id: instance.id, workflow_type: instance.workflow_type}
              )

              {:ok, instance}

            {:error, reason} ->
              # Row is durable and reclaimable; Recovery will start the runner.
              emit_runner_start_failed(instance.id, :pending, reason)

              :telemetry.execute(
                [:interruptus, :workflow, :started],
                %{},
                %{workflow_id: instance.id, workflow_type: instance.workflow_type}
              )

              {:ok, instance}
          end

        error ->
          error
      end
    end
  end

  @doc """
  Resumes a suspended or failed workflow by starting a new runner.

  Performs a **fenced** status transition first (bumping `lock_version`, which
  stops any stale runner at its next write):

    * `:suspended` → `:pending` — forward execution continues from the
      suspension point
    * `:failed` → `:compensating` — compensation is retried from the persisted
      `compensation_index`
    * other non-terminal statuses are left unchanged (a runner is simply
      ensured, e.g. after lease expiry)

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)

  ## Returns

    * `{:ok, pid()}` - runner process pid (new or existing)
    * `{:error, :not_found}` - no row with that id
    * `{:error, :terminal}` - workflow is `:completed`, `:compensated`, or `:cancelled`
    * `{:error, :stale_lock}` - concurrent update; safe to retry
    * `{:error, :unknown_workflow_type}` - `workflow_type` does not resolve to
      a loaded workflow module on this node
    * `{:error, :not_compensable}` - `:failed` workflow has an empty compensation
      plan; status stays `:failed`
    * `{:error, :in_transaction}` - called inside an open transaction on the
      Interruptus-configured repo
    * `{:error, term()}` - runner could not be started
  """
  @spec resume(Ecto.UUID.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def resume(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    with :ok <- reject_in_transaction(config),
         %WorkflowInstance{} = instance <- Store.get(config, workflow_id),
         false <- WorkflowInstance.terminal?(instance),
         {:ok, workflow_module} <- WorkflowType.resolve(instance.workflow_type),
         {:ok, _prepared} <- prepare_resume(config, workflow_module, instance),
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

  The cancel write bumps `lock_version` (fencing token), so a live runner —
  even one holding a valid lease — fails its next write with `:stale_lock`
  and stops without further side effects on the workflow row. Any registered
  runner is always evicted after a successful cancel write.

  **Defaults to `compensate: true`**: when the compensation plan is non-empty,
  the workflow transitions to `:compensating` instead of `:cancelled`. Pass
  `compensate: false` with `force: true` to abandon compensation (operator
  accepts inconsistent external state).

  Cancel while `:compensating` requires `force: true` (`:compensation_in_progress`).

  Retries the fenced write a few times when it races runner writes.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)
    * `:compensate` - run compensations for passed/in-flight checkpoints
      (default `true`)
    * `:force` - allow abandoning compensation or interrupting `:compensating`
      (default `false`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - updated instance (`:cancelled`, or
      `:compensating` when compensating)
    * `{:error, :not_found}` - no row with that id
    * `{:error, :terminal}` - workflow is already terminal
    * `{:error, :compensation_required}` - plain cancel refused; use default
      compensate or `force: true`
    * `{:error, :compensation_in_progress}` - cancel during `:compensating`
      without `force: true`
    * `{:error, :stale_lock}` - persistent write races; safe to retry
    * `{:error, :in_transaction}` - called inside an open transaction on the
      Interruptus-configured repo
  """
  @spec cancel(Ecto.UUID.t(), keyword()) :: {:ok, WorkflowInstance.t()} | {:error, term()}
  def cancel(workflow_id, opts \\ []) do
    config = config_from_opts(opts)
    compensate? = Keyword.get(opts, :compensate, true)
    force? = Keyword.get(opts, :force, false)

    with :ok <- reject_in_transaction(config) do
      do_cancel(config, workflow_id, compensate?, force?, @cancel_retries)
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

  @doc """
  Resolves the segment name at a workflow index, or for a persisted instance.

  Checkpoint names default to the `verify` atom when set, otherwise the first
  pipeline atom. Pass an explicit `checkpoint :name do` (or `name:`) to override.
  Bare stages use their pipeline function atom.

  ## Arguments

    * When the first argument is a workflow module, the second is a
      non-negative segment index.
    * When the first argument is a workflow UUID, options may include `:config`.

  ## Returns

    * `{:ok, atom()}` - segment name
    * `{:ok, nil}` - index past the end of the pipeline (or empty name)
    * `{:error, :not_found}` - unknown workflow id
    * `{:error, :unknown_workflow_type}` - module not loaded on this node
  """
  @spec segment_name(module() | Ecto.UUID.t(), non_neg_integer() | keyword()) ::
          {:ok, atom() | nil} | {:error, term()}
  def segment_name(workflow_module, index)
      when is_atom(workflow_module) and is_integer(index) and index >= 0 do
    case Enum.at(workflow_module.flattened_pipelines(), index) do
      %{name: name} -> {:ok, name}
      nil -> {:ok, nil}
    end
  end

  def segment_name(workflow_id, opts) when is_binary(workflow_id) and is_list(opts) do
    with {:ok, instance} <- status(workflow_id, opts),
         {:ok, workflow_module} <- WorkflowType.resolve(instance.workflow_type) do
      segment_name(workflow_module, instance.current_stage_index)
    end
  end

  @spec segment_name(Ecto.UUID.t()) :: {:ok, atom() | nil} | {:error, term()}
  def segment_name(workflow_id) when is_binary(workflow_id) do
    segment_name(workflow_id, [])
  end

  ## Internal ---------------------------------------------------------------

  @spec insert_or_existing(Config.t(), String.t(), String.t() | nil, map()) ::
          {:ok, WorkflowInstance.t()} | {:error, Ecto.Changeset.t()}
  defp insert_or_existing(config, workflow_type, idempotency_key, attrs) do
    case Store.insert_workflow(config, attrs) do
      {:ok, instance} ->
        {:ok, instance}

      {:error, %Ecto.Changeset{} = changeset} ->
        with true <- idempotency_conflict?(changeset),
             true <- is_binary(idempotency_key),
             %WorkflowInstance{} = existing <-
               Store.get_by_idempotency_key(config, workflow_type, idempotency_key) do
          {:ok, existing}
        else
          _ -> {:error, changeset}
        end
    end
  end

  @spec idempotency_conflict?(Ecto.Changeset.t()) :: boolean()
  defp idempotency_conflict?(%Ecto.Changeset{errors: errors}) do
    Enum.any?(errors, fn
      {:idempotency_key, {_, meta}} -> Keyword.get(meta, :constraint) == :unique
      _ -> false
    end)
  end

  @spec prepare_resume(Config.t(), module(), WorkflowInstance.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock | :not_compensable}
  defp prepare_resume(config, _workflow_module, %WorkflowInstance{status: :suspended} = instance) do
    Store.update_with_lock(config, instance, %{
      status: :pending,
      suspend_reason: nil,
      suspend_metadata: nil,
      attempt_count: 0
    })
  end

  defp prepare_resume(config, workflow_module, %WorkflowInstance{status: :failed} = instance) do
    case Rollback.compensation_plan(workflow_module, instance.current_stage_index) do
      [] ->
        {:error, :not_compensable}

      _plan ->
        Store.update_with_lock(config, instance, %{
          status: :compensating,
          attempt_count: 0,
          locked_by: nil,
          locked_until: nil
        })
    end
  end

  defp prepare_resume(_config, _workflow_module, instance), do: {:ok, instance}

  @spec do_cancel(Config.t(), Ecto.UUID.t(), boolean(), boolean(), non_neg_integer()) ::
          {:ok, WorkflowInstance.t()} | {:error, term()}
  defp do_cancel(_config, _workflow_id, _compensate?, _force?, 0), do: {:error, :stale_lock}

  defp do_cancel(config, workflow_id, compensate?, force?, retries) do
    case Store.get(config, workflow_id) do
      nil ->
        {:error, :not_found}

      %WorkflowInstance{} = instance ->
        with {:ok, workflow_module} <- WorkflowType.resolve(instance.workflow_type),
             :ok <- validate_cancel(instance, workflow_module, compensate?, force?) do
          if compensate? do
            cancel_with_compensation(config, workflow_module, workflow_id, instance, compensate?, force?, retries)
          else
            plain_cancel(config, workflow_module, workflow_id, instance, compensate?, force?, retries)
          end
        else
          {:error, :unknown_workflow_type} = err ->
            # Allow force plain cancel of unresolvable types.
            if force? and not compensate? do
              plain_cancel(config, nil, workflow_id, instance, compensate?, force?, retries)
            else
              err
            end

          other ->
            other
        end
    end
  end

  @spec validate_cancel(WorkflowInstance.t(), module(), boolean(), boolean()) ::
          :ok | {:error, term()}
  defp validate_cancel(instance, workflow_module, compensate?, force?) do
    cond do
      WorkflowInstance.terminal?(instance) ->
        {:error, :terminal}

      instance.status == :compensating and not force? ->
        {:error, :compensation_in_progress}

      not compensate? and not force? ->
        plan = Rollback.compensation_plan(workflow_module, instance.current_stage_index)

        if plan == [] do
          :ok
        else
          {:error, :compensation_required}
        end

      true ->
        :ok
    end
  end

  @spec plain_cancel(
          Config.t(),
          module() | nil,
          Ecto.UUID.t(),
          WorkflowInstance.t(),
          boolean(),
          boolean(),
          non_neg_integer()
        ) :: {:ok, WorkflowInstance.t()} | {:error, term()}
  defp plain_cancel(config, workflow_module, workflow_id, instance, compensate?, force?, retries) do
    case Store.update_with_lock(config, instance, %{
           status: :cancelled,
           locked_by: nil,
           locked_until: nil
         }) do
      {:ok, cancelled} ->
        emit_cancelled(workflow_id)
        maybe_replace_runner(config, workflow_module, workflow_id)
        {:ok, cancelled}

      {:error, :stale_lock} ->
        do_cancel(config, workflow_id, compensate?, force?, retries - 1)
    end
  end

  @spec cancel_with_compensation(
          Config.t(),
          module(),
          Ecto.UUID.t(),
          WorkflowInstance.t(),
          boolean(),
          boolean(),
          non_neg_integer()
        ) ::
          {:ok, WorkflowInstance.t()} | {:error, term()}
  defp cancel_with_compensation(
         config,
         workflow_module,
         workflow_id,
         instance,
         compensate?,
         force?,
         retries
       ) do
    plan = Rollback.compensation_plan(workflow_module, instance.current_stage_index)

    if plan == [] do
      plain_cancel(config, workflow_module, workflow_id, instance, compensate?, force?, retries)
    else
      case Store.update_with_lock(config, instance, %{
             status: :compensating,
             attempt_count: 0,
             locked_by: nil,
             locked_until: nil,
             errors: Map.put(instance.errors, "cancelled", "true")
           }) do
        {:ok, compensating} ->
          emit_cancelled(workflow_id)

          case RunnerSupervisor.replace_runner(config, workflow_module, workflow_id) do
            {:ok, _pid} ->
              {:ok, compensating}

            {:error, reason} ->
              emit_runner_start_failed(workflow_id, :compensating, reason)
              {:ok, compensating}
          end

        {:error, :stale_lock} ->
          do_cancel(config, workflow_id, compensate?, force?, retries - 1)
      end
    end
  end

  @spec maybe_replace_runner(Config.t(), module() | nil, Ecto.UUID.t()) :: :ok
  defp maybe_replace_runner(_config, nil, _workflow_id), do: :ok

  defp maybe_replace_runner(config, workflow_module, workflow_id) do
    # Evict any live runner; do not start a new one for terminal :cancelled.
    registry = Interruptus.Config.registry_name(config)
    runner_sup = Interruptus.Config.runner_supervisor_name(config)

    case Registry.lookup(registry, workflow_id) do
      [{pid, _}] ->
        _ = DynamicSupervisor.terminate_child(runner_sup, pid)
        :ok

      [] ->
        :ok
    end

    # Silence unused warning when module is only needed for type symmetry.
    _ = workflow_module
    :ok
  end

  @spec emit_runner_start_failed(Ecto.UUID.t(), atom(), term()) :: :ok
  defp emit_runner_start_failed(workflow_id, status, reason) do
    :telemetry.execute(
      [:interruptus, :workflow, :runner_start_failed],
      %{},
      %{workflow_id: workflow_id, status: status, reason: reason}
    )
  end

  @spec emit_cancelled(Ecto.UUID.t()) :: :ok
  defp emit_cancelled(workflow_id) do
    :telemetry.execute(
      [:interruptus, :workflow, :cancelled],
      %{},
      %{workflow_id: workflow_id}
    )
  end

  @spec config_from_opts(keyword()) :: Config.t()
  defp config_from_opts(opts) do
    opts
    |> Keyword.get(:config, Interruptus)
    |> Config.fetch()
  end

  @spec reject_in_transaction(Config.t()) :: :ok | {:error, :in_transaction}
  defp reject_in_transaction(config) do
    if Interruptus.Repo.in_transaction?(config) do
      {:error, :in_transaction}
    else
      :ok
    end
  end
end
