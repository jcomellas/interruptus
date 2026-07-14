defmodule Interruptus.Runner do
  @moduledoc """
  GenServer that executes a workflow instance with checkpoint persistence.

  One runner exists per active workflow id, registered in `Interruptus.Registry`.
  The execution loop:

  1. **Claim** — acquire lease via `Interruptus.Claim.acquire/2`
  2. **Execute** — run segments from `current_stage_index` via `Interruptus.Engine`
  3. **Checkpoint** — persist state after each checkpoint segment
  4. **Heartbeat** — renew lease on interval from config
  5. **Complete / Suspend / Fail** — terminal transitions with telemetry

  On failure, applies `Interruptus.Policy.Restart` then `Interruptus.Policy.Rollback`
  when retries are exhausted.
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

  @typep state :: %{
           config: Config.t(),
           workflow_module: module(),
           workflow_id: Ecto.UUID.t(),
           instance: WorkflowInstance.t() | nil
         }

  @typep workflow_command :: struct()

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
  # with reloaded state. Permanent/transient restarts would (a) resurrect a
  # runner with stale in-memory state and (b) let a crash-looping runner trip
  # the supervisor restart intensity, cascading up and taking down the shared
  # Interruptus.Registry.
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

  # GenServer init: registers in Registry, trap_exit, schedules :run.
  @doc false
  @spec init(keyword()) :: {:ok, state()}
  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    workflow_module = Keyword.fetch!(opts, :workflow_module)
    workflow_id = Keyword.fetch!(opts, :workflow_id)

    Registry.register(Interruptus.Registry, workflow_id, workflow_module)

    Process.flag(:trap_exit, true)

    send(self(), :run)

    {:ok,
     %{
       config: config,
       workflow_module: workflow_module,
       workflow_id: workflow_id,
       instance: nil
     }}
  end

  # Handles :run — begins or resumes the claim-and-execute loop.
  @doc false
  @spec handle_info(:run, state()) :: {:noreply, state()}
  @impl true
  def handle_info(:run, state) do
    {:noreply, execute(state)}
  end

  # Handles :heartbeat — renews the lease or stops if renewal fails.
  @doc false
  @spec handle_info(:heartbeat, state()) :: {:noreply, state()} | {:stop, :lease_lost, state()}
  def handle_info(:heartbeat, %{config: config, instance: %WorkflowInstance{} = instance} = state) do
    case Claim.renew(config, instance) do
      {:ok, renewed} ->
        schedule_heartbeat(config)
        {:noreply, %{state | instance: renewed}}

      {:error, _} ->
        {:stop, :lease_lost, state}
    end
  end

  # Handles {:retry, attempt} — schedules another execution after backoff.
  @doc false
  @spec handle_info({:retry, non_neg_integer()}, state()) :: {:noreply, state()}
  def handle_info({:retry, attempt}, state) do
    send(self(), :run)
    {:noreply, put_in(state.instance.attempt_count, attempt)}
  end

  # Handles :stop — normal shutdown after completion or compensation.
  @doc false
  @spec handle_info(:stop, state()) :: {:stop, :normal, state()}
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  # Handles linked process exits without stopping the runner.
  @doc false
  @spec handle_info({:EXIT, pid(), term()}, state()) :: {:noreply, state()}
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  # GenServer terminate: releases lease and emits telemetry.
  @doc false
  @spec terminate(term(), state()) :: :ok
  @impl true
  def terminate(_reason, %{config: config, instance: %WorkflowInstance{} = instance}) do
    :telemetry.execute(
      [:interruptus, :runner, :terminate],
      %{},
      %{workflow_id: instance.id, status: instance.status}
    )

    Claim.release(config, instance)
    :ok
  end

  @doc false
  def terminate(_reason, _state), do: :ok

  @spec execute(state()) :: state()
  defp execute(%{config: config, workflow_module: workflow_module, workflow_id: workflow_id} = state) do
    with {:ok, instance} <- Claim.acquire(config, workflow_id),
         {:ok, command} <- build_command(workflow_module, instance) do
      schedule_heartbeat(config)

      :telemetry.execute(
        [:interruptus, :workflow, :claimed],
        %{},
        %{workflow_id: workflow_id, node_id: config.node_id}
      )

      state
      |> Map.put(:instance, instance)
      |> run_loop(command, instance.current_stage_index)
    else
      {:error, %CastError{} = error, instance} ->
        fail_on_cast_error(%{state | instance: instance}, error)

      {:error, _} ->
        state
    end
  end

  @spec run_loop(state(), workflow_command(), non_neg_integer()) :: state()
  defp run_loop(state, command, stage_index) do
    %{workflow_module: workflow_module} = state
    segments = workflow_module.flattened_pipelines()

    if stage_index >= length(segments) do
      complete_workflow(state, command)
    else
      segment = Enum.at(segments, stage_index)

      case Engine.run_segment(workflow_module, segment, command, timeout: stage_timeout(workflow_module)) do
        {:ok, updated} ->
          if segment.type == :checkpoint do
            checkpoint_and_continue(state, updated, stage_index + 1)
          else
            run_loop(state, updated, stage_index + 1)
          end

        {:suspend, reason, metadata, updated} ->
          suspend_workflow(state, updated, reason, metadata, stage_index)

        {:halted, halted} ->
          handle_failure(state, halted, :halted)

        {:error, reason} ->
          handle_failure(state, command, reason)
      end
    end
  end

  @spec checkpoint_and_continue(state(), workflow_command(), non_neg_integer()) :: state()
  defp checkpoint_and_continue(state, command, next_index) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, updated_instance} <-
           Store.checkpoint_progress(config, instance, %{
             params: params,
             data: data,
             current_stage_index: next_index,
             errors: command.errors
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :checkpoint],
        %{stage_index: next_index},
        %{workflow_id: instance.id}
      )

      run_loop(%{state | instance: updated_instance}, command, next_index)
    else
      {:error, :stale_lock} ->
        state

      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, _} ->
        handle_failure(state, command, :checkpoint_failed)
    end
  end

  @spec complete_workflow(state(), workflow_command()) :: state()
  defp complete_workflow(state, command) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, completed} <-
           Store.update_with_lock(config, instance, %{
             status: :completed,
             params: params,
             data: data,
             current_stage_index: workflow_module.flattened_pipelines() |> length(),
             locked_by: nil,
             locked_until: nil,
             errors: %{}
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :completed],
        %{},
        %{workflow_id: completed.id}
      )

      Process.send(self(), :stop, [])
      %{state | instance: completed}
    else
      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, _} ->
        state
    end
  end

  @spec suspend_workflow(state(), workflow_command(), term(), map(), non_neg_integer()) :: state()
  defp suspend_workflow(state, command, reason, metadata, index) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    with {:ok, params} <- workflow_module.dump_params(command.params),
         {:ok, data} <- workflow_module.dump_data(command.data),
         {:ok, suspended} <-
           Store.update_with_lock(config, instance, %{
             status: :suspended,
             current_stage_index: index,
             params: params,
             data: data,
             suspend_reason: to_string(reason),
             suspend_metadata: metadata,
             locked_by: nil,
             locked_until: nil
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :suspended],
        %{},
        %{workflow_id: suspended.id, reason: reason}
      )

      Process.send(self(), :stop, [])
      %{state | instance: suspended}
    else
      {:error, %CastError{} = error} ->
        fail_on_cast_error(state, error)

      {:error, _} ->
        state
    end
  end

  @spec handle_failure(state(), workflow_command(), term()) :: state()
  defp handle_failure(state, _command, %CastError{} = reason) do
    fail_on_cast_error(state, reason)
  end

  defp handle_failure(state, command, reason) do
    %{config: config, workflow_module: workflow_module, instance: instance} = state
    policy = workflow_module.restart_policy()
    attempt = instance.attempt_count + 1

    _ =
      Store.log_attempt(config, %{
        workflow_id: instance.id,
        stage_name: stage_name(instance),
        attempt_number: attempt,
        outcome: outcome_for(reason),
        error: %{reason: inspect(reason)}
      })

    if Restart.retry?(policy, attempt) and Restart.retryable?(policy, reason) do
      delay = Restart.backoff_ms(policy, attempt)

      Store.update_with_lock(config, instance, %{attempt_count: attempt})

      :telemetry.execute(
        [:interruptus, :workflow, :retry],
        %{attempt: attempt, delay: delay},
        %{workflow_id: instance.id}
      )

      Process.send_after(self(), {:retry, attempt}, delay)
      state
    else
      rollback_or_fail(state, command, reason)
    end
  end

  @spec rollback_or_fail(state(), workflow_command(), term()) :: state()
  defp rollback_or_fail(state, command, reason) do
    %{config: config, workflow_module: workflow_module, instance: instance} = state
    compensate_fns = workflow_module.rollback_policy().compensate

    Store.update_with_lock(config, instance, %{status: :compensating})

    case Rollback.compensate(workflow_module, command, compensate_fns) do
      {:ok, _} ->
        Store.update_with_lock(config, instance, %{
          status: :compensated,
          locked_by: nil,
          locked_until: nil
        })

        :telemetry.execute(
          [:interruptus, :workflow, :compensated],
          %{},
          %{workflow_id: instance.id}
        )

        Process.send(self(), :stop, [])
        state

      {:error, _} ->
        Store.update_with_lock(config, instance, %{
          status: :failed,
          locked_by: nil,
          locked_until: nil,
          errors: Map.put(instance.errors, "failure", inspect(reason))
        })

        :telemetry.execute(
          [:interruptus, :workflow, :failed],
          %{},
          %{workflow_id: instance.id, reason: reason}
        )

        Process.send(self(), :stop, [])
        state
    end
  end

  @spec build_command(module(), WorkflowInstance.t()) ::
          {:ok, workflow_command()} | {:error, CastError.t(), WorkflowInstance.t()}
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

  @spec fail_on_cast_error(state(), CastError.t()) :: state()
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

    _ =
      Store.update_with_lock(config, instance, %{
        status: :failed,
        locked_by: nil,
        locked_until: nil,
        errors: Map.put(instance.errors, "cast", CastError.encode(error))
      })

    Process.send(self(), :stop, [])
    state
  end

  @spec schedule_heartbeat(Config.t()) :: reference()
  defp schedule_heartbeat(%Config{heartbeat_interval: interval}) do
    Process.send_after(self(), :heartbeat, interval)
  end

  @spec stage_timeout(module()) :: :infinity
  defp stage_timeout(_workflow_module), do: :infinity

  @spec stage_name(WorkflowInstance.t()) :: String.t()
  defp stage_name(%WorkflowInstance{current_stage_index: index}), do: "stage_#{index}"

  @spec outcome_for(term()) :: attempt_outcome()
  defp outcome_for(:halted), do: :halted
  defp outcome_for(:timeout), do: :timeout
  defp outcome_for(:verify_failed), do: :verify_failed
  defp outcome_for(_), do: :failure
end
