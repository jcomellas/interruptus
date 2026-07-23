defmodule Interruptus.Runner do
  @moduledoc """
  GenServer that executes a workflow instance with checkpoint persistence.

  One runner exists per active workflow id on a node, registered in the
  per-instance Registry. The execution loop:

  1. **Claim** — acquire lease via `Interruptus.Claim.acquire/2`; a runner that
     cannot claim stops immediately (no idle processes holding registry slots)
  2. **Attempt accounting** — persist `attempt_count + 1` **before** executing,
     so crash loops are bounded across process deaths; the budget resets to 0
     at every successful checkpoint
  3. **Execute** — run the current segment in a `Task.Supervisor` task so this
     GenServer keeps processing heartbeats while stages run
  4. **Checkpoint** — persist state after each checkpoint segment with a
     holder-guarded fenced write
  5. **Heartbeat** — renew the lease on interval, concurrently with execution
  6. **Complete / Suspend / Fail / Compensate** — terminal transitions with
     telemetry

  ## Failure handling

  Stage errors — including raised exceptions, throws, exits, timeouts, halts,
  and `verify` failures — flow through `Interruptus.Policy.Restart`: bounded
  retries with backoff, then compensation via `Interruptus.Policy.Rollback`.
  Compensation executes one function at a time, persisting
  `compensation_index` after each success, so a crash mid-compensation is
  reclaimed and resumed rather than stranded.

  ## Fencing

  All writes are holder-guarded (`Interruptus.Store.update_as_holder/4`): a
  runner whose lease expired or whose row was fenced (by `Interruptus.cancel/2`,
  `Interruptus.resume/2`, or a re-claim on another node) fails its next write
  with `:stale_lock` and stops cleanly without mutating workflow state.
  """

  use GenServer

  require Logger

  alias Interruptus.Claim
  alias Interruptus.Config
  alias Interruptus.Engine
  alias Interruptus.Policy.Restart
  alias Interruptus.Policy.Rollback
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store
  alias Interruptus.Workflow.CastError

  @typep phase :: :starting | :running | :compensating

  @typep state :: %{
           config: Config.t(),
           workflow_module: module(),
           workflow_id: Ecto.UUID.t(),
           instance: WorkflowInstance.t() | nil,
           command: struct() | nil,
           exec_index: non_neg_integer(),
           phase: phase(),
           task: Task.t() | nil,
           comp_plan: [atom()] | nil
         }

  @typep step :: {:noreply, state()} | {:stop, term(), state()}

  @typep attempt_outcome :: :halted | :timeout | :verify_failed | :failure

  # Starts a runner GenServer. Options: :config, :workflow_module, :workflow_id.
  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # Runners are :temporary — a crashed runner is NOT auto-restarted by the
  # DynamicSupervisor. Recovery of a crashed instance happens through lease
  # expiry + Interruptus.Recovery, which re-claims and starts a fresh runner
  # with reloaded state (and a durably incremented attempt budget).
  @doc false
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  # GenServer init: registers in the per-instance Registry, trap_exit,
  # schedules :run. Returns :ignore when another runner won the registration
  # race for this workflow id.
  @doc false
  @spec init(keyword()) :: {:ok, state()} | :ignore
  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    workflow_module = Keyword.fetch!(opts, :workflow_module)
    workflow_id = Keyword.fetch!(opts, :workflow_id)

    case Registry.register(Config.registry_name(config), workflow_id, workflow_module) do
      {:ok, _} ->
        Process.flag(:trap_exit, true)

        send(self(), :run)

        {:ok,
         %{
           config: config,
           workflow_module: workflow_module,
           workflow_id: workflow_id,
           instance: nil,
           command: nil,
           exec_index: 0,
           phase: :starting,
           task: nil,
           comp_plan: nil
         }}

      {:error, {:already_registered, _pid}} ->
        :ignore
    end
  end

  # Handles :run — claims the workflow and starts execution or compensation.
  @doc false
  @impl true
  @spec handle_info(term(), state()) :: step()
  def handle_info(:run, state) do
    claim_and_start(state)
  end

  # Handles :heartbeat — renews the lease or stops when the lease is lost.
  def handle_info(:heartbeat, %{instance: %WorkflowInstance{} = instance} = state) do
    %{config: config} = state

    if instance.locked_by == config.node_id do
      case Claim.renew(config, instance) do
        {:ok, renewed} ->
          schedule_heartbeat(config)
          {:noreply, %{state | instance: renewed}}

        {:error, _} ->
          shutdown_task(state)
          {:stop, :lease_lost, %{state | task: nil}}
      end
    else
      # A terminal write already released the lease; :stop is queued.
      {:noreply, state}
    end
  end

  def handle_info(:heartbeat, state), do: {:noreply, state}

  # Handles :retry — reloads the row, verifies the lease is intact, and starts
  # the next attempt (forward or compensating). The runner already holds the
  # lease, so no re-claim happens.
  def handle_info(:retry, %{config: config, workflow_id: workflow_id} = state) do
    fresh = Store.get(config, workflow_id)

    cond do
      is_nil(fresh) ->
        {:stop, :normal, state}

      fresh.locked_by != config.node_id or fresh.lock_version != state.instance.lock_version ->
        stop_after_fence(state)

      state.phase == :compensating ->
        begin_comp_attempt(%{state | instance: fresh})

      true ->
        restart_from_checkpoint(%{state | instance: fresh})
    end
  end

  # Handles :stop — normal shutdown after a terminal transition.
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  # Handles the current segment/compensation task reply.
  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | task: nil}

    case state.phase do
      :compensating -> handle_comp_result(state, result)
      _ -> handle_segment_result(state, result)
    end
  end

  # Handles an abnormal exit of the current task (e.g. brutal kill).
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    state = %{state | task: nil}

    case state.phase do
      :compensating -> handle_comp_failure(state, {:task_exit, reason})
      _ -> handle_failure(state, {:task_exit, reason})
    end
  end

  # Ignores stale task replies and monitors from superseded tasks.
  def handle_info({ref, _result}, state) when is_reference(ref), do: {:noreply, state}
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  # Handles linked process exits without stopping the runner.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  # GenServer terminate: kills any in-flight task, releases the lease when
  # still held, and emits telemetry.
  @doc false
  @spec terminate(term(), state()) :: :ok
  @impl true
  def terminate(_reason, %{instance: %WorkflowInstance{} = instance} = state) do
    shutdown_task(state)

    :telemetry.execute(
      [:interruptus, :runner, :terminate],
      %{},
      %{workflow_id: instance.id, status: instance.status}
    )

    if instance.locked_by == state.config.node_id and not WorkflowInstance.terminal?(instance) do
      _ = Claim.release(state.config, instance)
    end

    :ok
  end

  @doc false
  def terminate(_reason, state) do
    shutdown_task(state)
    :ok
  end

  ## Claim ------------------------------------------------------------------

  @spec claim_and_start(state()) :: step()
  defp claim_and_start(state) do
    %{config: config, workflow_module: workflow_module, workflow_id: workflow_id} = state

    case Claim.acquire(config, workflow_id) do
      {:ok, instance} ->
        :telemetry.execute(
          [:interruptus, :workflow, :claimed],
          %{},
          %{workflow_id: workflow_id, node_id: config.node_id}
        )

        schedule_heartbeat(config)
        state = %{state | instance: instance}

        if instance.pipeline_version == workflow_module.pipeline_version() do
          start_from_claim(state)
        else
          park_version_mismatch(state)
        end

      {:error, _reason} ->
        # Not claimable (terminal, suspended, held elsewhere) or missing.
        # Stop immediately so the Registry slot is freed for future resumes.
        {:stop, :normal, state}
    end
  end

  @spec start_from_claim(state()) :: step()
  defp start_from_claim(%{instance: instance, workflow_module: workflow_module} = state) do
    case build_command(workflow_module, instance) do
      {:ok, command} ->
        state = %{state | command: command, exec_index: instance.current_stage_index}

        case instance.status do
          :compensating ->
            plan = Rollback.compensation_plan(workflow_module, instance.current_stage_index)
            begin_comp_attempt(%{state | phase: :compensating, comp_plan: plan})

          _ ->
            begin_attempt(%{state | phase: :running})
        end

      {:error, %CastError{} = error, _instance} ->
        fail_on_cast_error(state, error)
    end
  end

  # Deploy-skew guard: never execute positional stage indexes recorded by a
  # different pipeline layout. Parks as :suspended for operator action.
  @spec park_version_mismatch(state()) :: step()
  defp park_version_mismatch(state) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    stored = instance.pipeline_version
    compiled = workflow_module.pipeline_version()

    Logger.warning(
      "interruptus pipeline version mismatch workflow_id=#{instance.id} " <>
        "stored=#{stored} compiled=#{compiled}; parking as :suspended"
    )

    :telemetry.execute(
      [:interruptus, :workflow, :version_mismatch],
      %{},
      %{workflow_id: instance.id, stored: stored, compiled: compiled}
    )

    write =
      Store.update_as_holder(config, instance, config.node_id, %{
        status: :suspended,
        suspend_reason: "pipeline_version_mismatch",
        suspend_metadata: %{"stored" => stored, "compiled" => compiled},
        locked_by: nil,
        locked_until: nil
      })

    case write do
      {:ok, parked} -> {:stop, :normal, %{state | instance: parked}}
      {:error, :stale_lock} -> stop_after_fence(state)
    end
  end

  ## Forward execution ------------------------------------------------------

  # Persists the attempt (durable, pre-execution) and starts the segment task.
  # When the persisted budget is already exhausted (e.g. repeated crashes),
  # goes straight to rollback.
  @spec begin_attempt(state()) :: step()
  defp begin_attempt(state) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    if state.exec_index >= length(workflow_module.flattened_pipelines()) do
      complete_workflow(state)
    else
      policy = workflow_module.restart_policy()
      attempt = instance.attempt_count + 1

      if attempt > policy.max_attempts do
        start_rollback(state, :attempts_exhausted)
      else
        case Store.update_as_holder(config, instance, config.node_id, %{attempt_count: attempt}) do
          {:ok, updated} -> start_segment(%{state | instance: updated})
          {:error, :stale_lock} -> stop_after_fence(state)
        end
      end
    end
  end

  @spec start_segment(state()) :: step()
  defp start_segment(state) do
    %{config: config, workflow_module: workflow_module, command: command} = state

    segments = workflow_module.flattened_pipelines()

    if state.exec_index >= length(segments) do
      complete_workflow(state)
    else
      segment = Enum.at(segments, state.exec_index)
      timeout = workflow_module.stage_timeout()

      task =
        Task.Supervisor.async_nolink(Config.task_supervisor_name(config), fn ->
          Engine.run_segment(workflow_module, segment, command, timeout: timeout)
        end)

      {:noreply, %{state | task: task}}
    end
  end

  @spec handle_segment_result(state(), term()) :: step()
  defp handle_segment_result(state, result) do
    %{workflow_module: workflow_module} = state
    segment = Enum.at(workflow_module.flattened_pipelines(), state.exec_index)

    case result do
      {:ok, updated} ->
        state = %{state | command: updated}

        if segment.type == :checkpoint do
          checkpoint_and_continue(state)
        else
          start_segment(%{state | exec_index: state.exec_index + 1})
        end

      {:suspend, reason, metadata, updated} ->
        suspend_workflow(%{state | command: updated}, reason, metadata)

      {:halted, halted} ->
        if Map.get(halted, :success, false) do
          complete_workflow(%{state | command: halted})
        else
          handle_failure(%{state | command: halted}, :halted)
        end

      {:error, reason, failed_command} ->
        handle_failure(%{state | command: failed_command}, reason)

      {:error, reason} ->
        handle_failure(state, reason)
    end
  end

  @spec checkpoint_and_continue(state()) :: step()
  defp checkpoint_and_continue(state) do
    %{config: config, instance: instance, workflow_module: workflow_module, command: command} =
      state

    next_index = state.exec_index + 1

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, updated_instance} <-
           Store.checkpoint_progress(config, instance, config.node_id, %{
             params: params,
             data: data,
             current_stage_index: next_index,
             attempt_count: 0,
             errors: command.errors
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :checkpoint],
        %{stage_index: next_index},
        %{workflow_id: instance.id}
      )

      # A checkpoint ends the current attempt span: the next segment starts a
      # fresh, durably persisted attempt (or completes the workflow).
      begin_attempt(%{state | instance: updated_instance, exec_index: next_index})
    else
      {:error, :stale_lock} ->
        stop_after_fence(state)

      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, _} ->
        handle_failure(state, :checkpoint_failed)
    end
  end

  @spec complete_workflow(state()) :: step()
  defp complete_workflow(state) do
    %{config: config, instance: instance, workflow_module: workflow_module, command: command} =
      state

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, completed} <-
           Store.update_as_holder(config, instance, config.node_id, %{
             status: :completed,
             params: params,
             data: data,
             current_stage_index: workflow_module.flattened_pipelines() |> length(),
             locked_by: nil,
             locked_until: nil,
             errors: command.errors
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :completed],
        %{},
        %{workflow_id: completed.id}
      )

      {:stop, :normal, %{state | instance: completed}}
    else
      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, :stale_lock} ->
        stop_after_fence(state)

      {:error, _} ->
        stop_after_fence(state)
    end
  end

  @spec suspend_workflow(state(), term(), map()) :: step()
  defp suspend_workflow(state, reason, metadata) do
    %{config: config, instance: instance, workflow_module: workflow_module, command: command} =
      state

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, suspended} <-
           Store.update_as_holder(config, instance, config.node_id, %{
             status: :suspended,
             current_stage_index: state.exec_index,
             params: params,
             data: data,
             attempt_count: 0,
             suspend_reason: reason_to_string(reason),
             suspend_metadata: metadata,
             locked_by: nil,
             locked_until: nil
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :suspended],
        %{},
        %{workflow_id: suspended.id, reason: reason}
      )

      {:stop, :normal, %{state | instance: suspended}}
    else
      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, :stale_lock} ->
        stop_after_fence(state)
    end
  end

  ## Failure and retry ------------------------------------------------------

  @spec handle_failure(state(), term()) :: step()
  defp handle_failure(state, %CastError{} = reason) do
    fail_on_cast_error(state, reason)
  end

  defp handle_failure(state, reason) do
    %{config: config, workflow_module: workflow_module, instance: instance} = state
    policy = workflow_module.restart_policy()
    attempt = instance.attempt_count

    _ =
      Store.log_attempt(config, %{
        workflow_id: instance.id,
        stage_name: segment_label(workflow_module, state.exec_index),
        attempt_number: attempt,
        outcome: outcome_for(reason),
        error: %{reason: format_reason(reason)}
      })

    if Restart.retry?(policy, attempt) and Restart.retryable?(policy, reason) do
      schedule_retry(state, policy, attempt)
    else
      start_rollback(state, reason)
    end
  end

  @spec schedule_retry(state(), Restart.t(), non_neg_integer()) :: step()
  defp schedule_retry(state, policy, attempt) do
    delay = Restart.backoff_ms(policy, attempt)

    :telemetry.execute(
      [:interruptus, :workflow, :retry],
      %{attempt: attempt, delay: delay},
      %{workflow_id: state.instance.id}
    )

    Process.send_after(self(), :retry, delay)
    {:noreply, state}
  end

  # Re-runs from the last durable checkpoint: discards partial in-memory
  # command state and rebuilds it from the persisted snapshot.
  @spec restart_from_checkpoint(state()) :: step()
  defp restart_from_checkpoint(state) do
    %{instance: instance, workflow_module: workflow_module} = state

    case build_command(workflow_module, instance) do
      {:ok, command} ->
        begin_attempt(%{
          state
          | command: command,
            exec_index: instance.current_stage_index,
            phase: :running
        })

      {:error, %CastError{} = error, _instance} ->
        fail_on_cast_error(state, error)
    end
  end

  ## Compensation -----------------------------------------------------------

  @spec start_rollback(state(), term()) :: step()
  defp start_rollback(state, reason) do
    %{config: config, instance: instance, workflow_module: workflow_module, command: command} =
      state

    plan = Rollback.compensation_plan(workflow_module, instance.current_stage_index)
    errors = Map.put(instance.errors, "failure", format_reason(reason))

    if plan == [] do
      mark_failed(state, reason)
    else
      with {:ok, params} <- workflow_module.dump_params(command.params),
           {:ok, data} <- workflow_module.dump_data(command.data),
           {:ok, updated} <-
             Store.update_as_holder(config, instance, config.node_id, %{
               status: :compensating,
               attempt_count: 0,
               params: params,
               data: data,
               errors: errors
             }) do
        :telemetry.execute(
          [:interruptus, :workflow, :compensating],
          %{},
          %{workflow_id: instance.id, reason: reason}
        )

        begin_comp_attempt(%{
          state
          | instance: updated,
            phase: :compensating,
            comp_plan: plan
        })
      else
        {:error, %CastError{} = error} ->
          fail_on_cast_error(state, error)

        {:error, :stale_lock} ->
          stop_after_fence(state)
      end
    end
  end

  # Persists the compensation attempt (durable, pre-execution) and starts the
  # compensation step task. Exhausted budgets mark the workflow :failed;
  # Interruptus.resume/2 retries compensation from compensation_index.
  @spec begin_comp_attempt(state()) :: step()
  defp begin_comp_attempt(state) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    plan = state.comp_plan || Rollback.compensation_plan(workflow_module, instance.current_stage_index)
    state = %{state | comp_plan: plan}

    cond do
      plan == [] ->
        # Nothing was (or can be) rolled back — never invent :compensated.
        mark_failed(state, :not_compensable)

      instance.compensation_index >= length(plan) ->
        finish_compensation(state)

      true ->
        policy = workflow_module.restart_policy()
        attempt = instance.attempt_count + 1

        if attempt > policy.max_attempts do
          mark_failed(state, :compensation_exhausted)
        else
          case Store.update_as_holder(config, instance, config.node_id, %{attempt_count: attempt}) do
            {:ok, updated} -> start_comp_task(%{state | instance: updated})
            {:error, :stale_lock} -> stop_after_fence(state)
          end
        end
    end
  end

  @spec start_comp_task(state()) :: step()
  defp start_comp_task(state) do
    %{config: config, instance: instance, workflow_module: workflow_module, command: command} =
      state

    fn_ref = Enum.at(state.comp_plan, instance.compensation_index)
    timeout = workflow_module.stage_timeout()

    task =
      Task.Supervisor.async_nolink(Config.task_supervisor_name(config), fn ->
        run_with_timeout(fn -> Rollback.apply_step(workflow_module, fn_ref, command) end, timeout)
      end)

    {:noreply, %{state | task: task}}
  end

  @spec handle_comp_result(state(), term()) :: step()
  defp handle_comp_result(state, {:ok, updated_command}) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state
    state = %{state | command: updated_command}

    with {:ok, params} <- workflow_module.dump_params(updated_command.params),
         {:ok, data} <- workflow_module.dump_data(updated_command.data),
         {:ok, updated} <-
           Store.update_as_holder(config, instance, config.node_id, %{
             compensation_index: instance.compensation_index + 1,
             attempt_count: 0,
             params: params,
             data: data
           }) do
      begin_comp_attempt(%{state | instance: updated})
    else
      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, :stale_lock} ->
        stop_after_fence(state)
    end
  end

  defp handle_comp_result(state, {:error, reason}) do
    handle_comp_failure(state, reason)
  end

  @spec handle_comp_failure(state(), term()) :: step()
  defp handle_comp_failure(state, reason) do
    %{config: config, workflow_module: workflow_module, instance: instance} = state
    policy = workflow_module.restart_policy()
    attempt = instance.attempt_count

    _ =
      Store.log_attempt(config, %{
        workflow_id: instance.id,
        stage_name: "compensation_#{instance.compensation_index}",
        attempt_number: attempt,
        outcome: outcome_for(reason),
        error: %{reason: format_reason(reason)}
      })

    if Restart.retry?(policy, attempt) and Restart.retryable?(policy, reason) do
      schedule_retry(state, policy, attempt)
    else
      mark_failed(state, {:compensation_failed, reason})
    end
  end

  @spec finish_compensation(state()) :: step()
  defp finish_compensation(state) do
    %{config: config, instance: instance} = state

    write =
      Store.update_as_holder(config, instance, config.node_id, %{
        status: :compensated,
        locked_by: nil,
        locked_until: nil
      })

    case write do
      {:ok, compensated} ->
        :telemetry.execute(
          [:interruptus, :workflow, :compensated],
          %{},
          %{workflow_id: instance.id}
        )

        {:stop, :normal, %{state | instance: compensated}}

      {:error, :stale_lock} ->
        stop_after_fence(state)
    end
  end

  ## Terminal writes --------------------------------------------------------

  @spec mark_failed(state(), term()) :: step()
  defp mark_failed(state, reason) do
    %{config: config, instance: instance} = state

    write =
      Store.update_as_holder(config, instance, config.node_id, %{
        status: :failed,
        locked_by: nil,
        locked_until: nil,
        errors: Map.put(instance.errors, "failure", format_reason(reason))
      })

    case write do
      {:ok, failed} ->
        :telemetry.execute(
          [:interruptus, :workflow, :failed],
          %{},
          %{workflow_id: instance.id, reason: reason}
        )

        {:stop, :normal, %{state | instance: failed}}

      {:error, :stale_lock} ->
        stop_after_fence(state)
    end
  end

  @spec fail_on_cast_error(state(), CastError.t()) :: step()
  defp fail_on_cast_error(%{config: config, instance: %WorkflowInstance{} = instance} = state, error) do
    Logger.error(
      "workflow cast error workflow_id=#{instance.id} field=#{error.field} operation=#{error.operation}: #{error.message}"
    )

    :telemetry.execute(
      [:interruptus, :workflow, :cast_failed],
      %{},
      %{
        workflow_id: instance.id,
        field: error.field,
        operation: error.operation,
        reason: error.reason
      }
    )

    write =
      Store.update_as_holder(config, instance, config.node_id, %{
        status: :failed,
        locked_by: nil,
        locked_until: nil,
        errors: Map.put(instance.errors, "cast", CastError.encode(error))
      })

    case write do
      {:ok, failed} -> {:stop, :normal, %{state | instance: failed}}
      {:error, :stale_lock} -> stop_after_fence(state)
    end
  end

  # The row was fenced: cancelled, resumed elsewhere, lease expired and
  # re-claimed, or otherwise concurrently modified. Never write; stop cleanly.
  @spec stop_after_fence(state()) :: step()
  defp stop_after_fence(state) do
    workflow_id = state.workflow_id

    Logger.info("interruptus runner fenced workflow_id=#{workflow_id}; stopping without writes")

    :telemetry.execute(
      [:interruptus, :workflow, :fenced],
      %{},
      %{workflow_id: workflow_id, node_id: state.config.node_id}
    )

    {:stop, :normal, state}
  end

  ## Helpers ----------------------------------------------------------------

  @spec build_command(module(), WorkflowInstance.t()) ::
          {:ok, struct()} | {:error, CastError.t(), WorkflowInstance.t()}
  defp build_command(workflow_module, %WorkflowInstance{} = instance) do
    with {:ok, params} <- workflow_module.load_params(instance.params),
         {:ok, loaded_data} <- workflow_module.load_data(instance.data) do
      base = struct(workflow_module)

      command = %{
        base
        | params: Map.merge(base.params, params),
          data: Map.merge(base.data, loaded_data),
          errors: instance.errors,
          workflow_id: instance.id
      }

      {:ok, command}
    else
      {:error, %CastError{} = error} ->
        {:error, error, instance}
    end
  end

  @spec run_with_timeout((-> term()), :infinity | pos_integer()) :: term()
  defp run_with_timeout(fun, :infinity), do: fun.()

  defp run_with_timeout(fun, timeout) when is_integer(timeout) do
    task = Task.async(fun)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, :timeout}
      {:exit, reason} -> {:error, {:exit, reason}}
    end
  end

  @spec shutdown_task(state()) :: :ok
  defp shutdown_task(%{task: %Task{} = task}) do
    _ = Task.shutdown(task, :brutal_kill)
    :ok
  end

  defp shutdown_task(_state), do: :ok

  @spec schedule_heartbeat(Config.t()) :: reference()
  defp schedule_heartbeat(%Config{heartbeat_interval: interval}) do
    Process.send_after(self(), :heartbeat, interval)
  end

  @spec segment_label(module(), non_neg_integer()) :: String.t()
  defp segment_label(workflow_module, index) do
    case Enum.at(workflow_module.flattened_pipelines(), index) do
      %{type: :stage, name: name} -> to_string(name)
      %{type: :checkpoint} -> "checkpoint_#{index}"
      nil -> "stage_#{index}"
    end
  end

  @spec reason_to_string(term()) :: String.t()
  defp reason_to_string(reason) when is_binary(reason), do: reason
  defp reason_to_string(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp reason_to_string(reason), do: inspect(reason)

  @spec format_reason(term()) :: String.t()
  defp format_reason({:exception, exception, stacktrace}) do
    Exception.format(:error, exception, stacktrace) |> String.slice(0, 4000)
  end

  defp format_reason(reason), do: inspect(reason)

  @spec outcome_for(term()) :: attempt_outcome()
  defp outcome_for(:halted), do: :halted
  defp outcome_for(:timeout), do: :timeout
  defp outcome_for(:verify_failed), do: :verify_failed
  defp outcome_for(_), do: :failure
end
