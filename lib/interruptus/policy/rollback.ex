defmodule Interruptus.Policy.Rollback do
  @moduledoc """
  Rollback policy: LIFO compensation invocation on terminal failure.
  """

  @doc """
  Runs compensation functions in LIFO order.

  Compensation entries may be atoms (module function names) or arity-1 functions.
  """
  @spec compensate(module(), struct(), [atom() | (struct() -> struct())]) ::
          {:ok, struct()} | {:error, term()}
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
