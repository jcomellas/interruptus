defmodule Interruptus.Policy.Rollback do
  @moduledoc """
  Rollback policy: LIFO compensation over passed checkpoints.

  When restart attempts are exhausted, compensation runs in two parts:

  1. Per-checkpoint compensations (`checkpoint compensate: :fun do ... end`)
     for checkpoints the workflow actually **passed** (their snapshot was
     persisted), in LIFO order.
  2. The workflow-level `rollback_policy compensate: [...]` list, also in LIFO
     order, appended after the per-checkpoint compensations.

  `Interruptus.Runner` executes the plan one function at a time, persisting
  `compensation_index` after each success, so a crash mid-compensation resumes
  from the last completed step instead of re-running (or abandoning) the whole
  plan.

  Each function receives the current command struct and must return an updated
  struct, `{:ok, struct}`, or `{:error, reason}`. Compensation functions must
  be idempotent: the step that was in flight during a crash runs again.
  """

  alias Interruptus.Command

  @type t :: %{
          compensate: [atom()]
        }

  @doc """
  Builds the ordered compensation plan for a workflow at a given progress point.

  ## Arguments

    * `workflow_module` - module using `Interruptus.Workflow`
    * `current_stage_index` - persisted `current_stage_index` of the instance;
      only checkpoints **before** this index are compensated

  ## Returns

    * Ordered list of compensation function atoms (may be empty)

  ## Examples

      iex> Interruptus.Policy.Rollback.compensation_plan(
      ...>   Interruptus.Test.Support.Workflows.Simple,
      ...>   0
      ...> )
      [:undo]
  """
  @spec compensation_plan(module(), non_neg_integer()) :: [atom()]
  def compensation_plan(workflow_module, current_stage_index) do
    checkpoint_compensations =
      workflow_module.flattened_pipelines()
      |> Enum.take(current_stage_index)
      |> Enum.filter(fn segment ->
        segment.type == :checkpoint and Map.get(segment, :compensate) != nil
      end)
      |> Enum.map(&Map.get(&1, :compensate))
      |> Enum.reverse()

    workflow_compensations =
      workflow_module.rollback_policy().compensate |> Enum.reverse()

    checkpoint_compensations ++ workflow_compensations
  end

  @doc """
  Applies a single compensation step, containing raises/throws/exits.

  ## Arguments

    * `workflow_module` - module using `Interruptus.Workflow`
    * `fn_ref` - function atom on the workflow module, or arity-1 function
    * `command` - command struct at failure time

  ## Returns

    * `{:ok, command}` - compensation succeeded
    * `{:error, term()}` - failure, invalid return, or contained crash
  """
  @spec apply_step(module(), atom() | (Command.t() -> Command.t()), Command.t()) ::
          {:ok, Command.t()} | {:error, term()}
  def apply_step(workflow_module, fn_ref, command) do
    apply_compensate(workflow_module, fn_ref, command)
  end

  @doc """
  Runs compensation functions in LIFO order.

  In-memory helper used by tests and pure execution; the durable runner steps
  through `compensation_plan/2` with `apply_step/3` instead.

  ## Arguments

    * `workflow_module` - module using `Interruptus.Workflow`
    * `command` - command struct at failure time
    * `compensate_fns` - list of atoms (module function names) or arity-1 functions

  ## Returns

    * `{:ok, command}` - all compensations succeeded
    * `{:error, term()}` - first compensation failure or invalid return value

  ## Examples

      iex> defmodule CompensateExample do
      ...>   def step1(cmd), do: %{cmd | data: Map.put(cmd.data, :undone, true)}
      ...>   def step2(cmd), do: cmd
      ...> end
      iex> cmd = %Interruptus.Test.Support.Workflows.Simple{
      ...>   data: %{result: 10},
      ...>   params: %{value: 5},
      ...>   errors: %{},
      ...>   halted: false,
      ...>   success: false,
      ...>   pipelines: []
      ...> }
      iex> {:ok, result} =
      ...>   Interruptus.Policy.Rollback.compensate(
      ...>     CompensateExample,
      ...>     cmd,
      ...>     [:step1, :step2]
      ...>   )
      iex> result.data.undone
      true
  """
  @spec compensate(module(), Command.t(), [atom() | (Command.t() -> Command.t())]) ::
          {:ok, Command.t()} | {:error, term()}
  def compensate(workflow_module, command, compensate_fns) do
    compensate_fns
    |> Enum.reverse()
    |> Enum.reduce_while({:ok, command}, fn fn_name, {:ok, acc} ->
      case apply_compensate(workflow_module, fn_name, acc) do
        {:ok, updated} -> {:cont, {:ok, updated}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec apply_compensate(module(), (Command.t() -> Command.t()) | atom(), Command.t()) ::
          {:ok, Command.t()} | {:error, term()}
  defp apply_compensate(_workflow_module, fun, command) when is_function(fun, 1) do
    normalize_result(fun.(command))
  rescue
    exception -> {:error, {:exception, exception, __STACKTRACE__}}
  catch
    :throw, value -> {:error, {:throw, value}}
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp apply_compensate(workflow_module, fn_name, command) do
    normalize_result(apply(workflow_module, fn_name, [command]))
  rescue
    exception -> {:error, {:exception, exception, __STACKTRACE__}}
  catch
    :throw, value -> {:error, {:throw, value}}
    :exit, reason -> {:error, {:exit, reason}}
  end

  @spec normalize_result(term()) :: {:ok, Command.t()} | {:error, term()}
  defp normalize_result(%{} = updated), do: {:ok, updated}
  defp normalize_result({:ok, updated}), do: {:ok, updated}
  defp normalize_result({:error, reason}), do: {:error, reason}
  defp normalize_result(other), do: {:error, {:invalid_compensation_result, other}}
end
