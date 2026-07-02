defmodule Interruptus.Policy.Rollback do
  @moduledoc """
  Rollback policy: LIFO compensation invocation on terminal failure.

  When restart attempts are exhausted, compensation functions from
  `rollback_policy/0` run in reverse definition order. Each function receives
  the current command struct and must return an updated struct or error tuple.
  """

  alias Interruptus.Command

  @type t :: %{
          compensate: [atom()]
        }

  @doc """
  Runs compensation functions in LIFO order.

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
    case fun.(command) do
      %{} = updated -> {:ok, updated}
      {:ok, updated} -> {:ok, updated}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_compensation_result, other}}
    end
  end

  defp apply_compensate(workflow_module, fn_name, command) do
    case apply(workflow_module, fn_name, [command]) do
      %{} = updated -> {:ok, updated}
      {:ok, updated} -> {:ok, updated}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_compensation_result, other}}
    end
  end
end
