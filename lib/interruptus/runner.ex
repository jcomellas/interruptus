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

  alias Interruptus.Claim
  alias Interruptus.Config
  alias Interruptus.Engine
  alias Interruptus.Policy.Restart
  alias Interruptus.Policy.Rollback
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  # Starts a runner GenServer. Options: :config, :workflow_module, :workflow_id.
  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  # GenServer init: registers in Registry, trap_exit, schedules :run.
  @doc false
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
  @impl true
  def handle_info(:run, state) do
    {:noreply, execute(state)}
  end

  # Handles :heartbeat — renews the lease or stops if renewal fails.
  @doc false
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
  def handle_info({:retry, attempt}, state) do
    send(self(), :run)
    {:noreply, put_in(state.instance.attempt_count, attempt)}
  end

  # Handles :stop — normal shutdown after completion or compensation.
  @doc false
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  # Handles linked process exits without stopping the runner.
  @doc false
  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  # GenServer terminate: releases lease and emits telemetry.
  @doc false
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

  defp execute(
         %{config: config, workflow_module: workflow_module, workflow_id: workflow_id} = state
       ) do
    with {:ok, instance} <- Claim.acquire(config, workflow_id),
         command <- build_command(workflow_module, instance) do
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
      {:error, _} ->
        state
    end
  end

  defp run_loop(state, command, stage_index) do
    %{workflow_module: workflow_module} = state
    segments = workflow_module.flattened_pipelines()

    if stage_index >= length(segments) do
      complete_workflow(state, command)
    else
      segment = Enum.at(segments, stage_index)

      case Engine.run_segment(workflow_module, segment, command,
             timeout: stage_timeout(workflow_module)
           ) do
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

  defp checkpoint_and_continue(state, command, next_index) do
    %{config: config, instance: instance} = state

    attrs = %{
      params: stringify_keys(command.params),
      data: stringify_keys(command.data),
      current_stage_index: next_index,
      errors: command.errors
    }

    with {:ok, updated_instance} <- Store.update_with_lock(config, instance, attrs),
         {:ok, _} <-
           Store.write_checkpoint(config, %{updated_instance | current_stage_index: next_index}) do
      :telemetry.execute(
        [:interruptus, :workflow, :checkpoint],
        %{stage_index: next_index},
        %{workflow_id: instance.id}
      )

      run_loop(%{state | instance: updated_instance}, command, next_index)
    else
      {:error, :stale_lock} ->
        state

      {:error, _} ->
        handle_failure(state, command, :checkpoint_failed)
    end
  end

  defp complete_workflow(state, command) do
    %{config: config, instance: instance, workflow_module: workflow_module} = state

    attrs = %{
      status: :completed,
      params: stringify_keys(command.params),
      data: stringify_keys(command.data),
      current_stage_index: workflow_module.flattened_pipelines() |> length(),
      locked_by: nil,
      locked_until: nil,
      errors: %{}
    }

    case Store.update_with_lock(config, instance, attrs) do
      {:ok, completed} ->
        :telemetry.execute(
          [:interruptus, :workflow, :completed],
          %{},
          %{workflow_id: completed.id}
        )

        Process.send(self(), :stop, [])
        %{state | instance: completed}

      {:error, _} ->
        state
    end
  end

  defp suspend_workflow(state, command, reason, metadata, index) do
    %{config: config, instance: instance} = state

    attrs = %{
      status: :suspended,
      current_stage_index: index,
      params: stringify_keys(command.params),
      data: stringify_keys(command.data),
      suspend_reason: to_string(reason),
      suspend_metadata: metadata,
      locked_by: nil,
      locked_until: nil
    }

    case Store.update_with_lock(config, instance, attrs) do
      {:ok, suspended} ->
        :telemetry.execute(
          [:interruptus, :workflow, :suspended],
          %{},
          %{workflow_id: suspended.id, reason: reason}
        )

        Process.send(self(), :stop, [])
        %{state | instance: suspended}

      {:error, _} ->
        state
    end
  end

  defp handle_failure(state, command, reason) do
    %{config: config, workflow_module: workflow_module, instance: instance} = state
    policy = workflow_module.restart_policy()
    attempt = instance.attempt_count + 1

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
          errors: Map.put(instance.errors || %{}, "failure", inspect(reason))
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

  defp build_command(workflow_module, %WorkflowInstance{} = instance) do
    params =
      instance.params
      |> atomize_keys()
      |> Enum.map(fn {k, v} -> {k, v} end)

    workflow_module.new(params)
    |> Map.put(:data, Map.merge(workflow_module.__struct__().data, atomize_keys(instance.data)))
    |> Map.put(:errors, instance.errors || %{})
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) ->
        try do
          {String.to_existing_atom(k), v}
        rescue
          ArgumentError -> {String.to_atom(k), v}
        end

      {k, v} ->
        {k, v}
    end)
  end

  defp schedule_heartbeat(%Config{heartbeat_interval: interval}) do
    Process.send_after(self(), :heartbeat, interval)
  end

  defp stage_timeout(_workflow_module), do: :infinity

  defp stage_name(%WorkflowInstance{current_stage_index: index}), do: "stage_#{index}"

  defp outcome_for(:halted), do: :halted
  defp outcome_for(:timeout), do: :timeout
  defp outcome_for(:verify_failed), do: :verify_failed
  defp outcome_for(_), do: :failure

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end
end
