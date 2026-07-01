defmodule Interruptus.Engine do
  @moduledoc """
  Pure in-memory execution engine for workflow segments.

  Handles verify-then-run logic for checkpoint segments and individual stages.
  """

  alias Interruptus.Command

  @type segment :: %{
          type: :stage | :checkpoint,
          name: atom() | nil,
          verify: atom() | nil,
          pipelines: [atom()]
        }

  @doc """
  Runs a single segment (verify + pipelines) against a command struct.
  """
  @spec run_segment(module(), segment(), struct(), keyword()) ::
          {:ok, struct()}
          | {:suspend, term(), map(), struct()}
          | {:halted, struct()}
          | {:error, term()}
  def run_segment(workflow_module, segment, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    case maybe_verify(workflow_module, segment, command) do
      {:skip, command} ->
        {:ok, command}

      {:run, command} ->
        run_pipelines(workflow_module, segment.pipelines, command, timeout)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Runs all segments from the given index to completion.
  """
  @spec run_from(module(), struct(), non_neg_integer(), keyword()) ::
          {:completed, struct()}
          | {:suspend, term(), map(), struct(), non_neg_integer()}
          | {:halted, struct(), non_neg_integer()}
          | {:error, term()}
  def run_from(workflow_module, command, from_index, opts \\ []) do
    segments = workflow_module.flattened_pipelines()
    do_run_from(workflow_module, command, segments, from_index, opts)
  end

  defp do_run_from(_workflow_module, command, segments, index, _opts)
       when index >= length(segments) do
    {:completed, Command.maybe_mark_successful(command)}
  end

  defp do_run_from(workflow_module, command, segments, index, opts) do
    segment = Enum.at(segments, index)

    case run_segment(workflow_module, segment, command, opts) do
      {:ok, updated} ->
        do_run_from(workflow_module, updated, segments, index + 1, opts)

      {:suspend, reason, metadata, updated} ->
        {:suspend, reason, metadata, updated, index}

      {:halted, halted} ->
        {:halted, halted, index}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_verify(_workflow_module, %{verify: nil}, command), do: {:run, command}

  defp maybe_verify(workflow_module, %{verify: verify}, command) do
    case apply(workflow_module, verify, [command]) do
      :done -> {:skip, command}
      :not_done -> {:run, command}
      :failed -> {:error, :verify_failed}
      other -> {:error, {:invalid_verify_result, other}}
    end
  end

  defp run_pipelines(_workflow_module, [], command, _timeout), do: {:ok, command}

  defp run_pipelines(workflow_module, [name | rest], command, timeout) do
    case run_stage(workflow_module, name, command, timeout) do
      {:ok, updated} ->
        if Map.get(updated, :halted, false) do
          {:halted, updated}
        else
          run_pipelines(workflow_module, rest, updated, timeout)
        end

      {:suspend, reason, metadata, updated} ->
        {:suspend, reason, metadata, updated}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp run_stage(_workflow_module, name, command, :infinity) do
    case Command.apply_fun(command, name) do
      {:suspend, reason, metadata} ->
        {:suspend, reason, metadata, command}

      %{} = result ->
        {:ok, result}

      other ->
        {:error, {:invalid_stage_result, other}}
    end
  end

  defp run_stage(_workflow_module, name, command, timeout) when is_integer(timeout) do
    task =
      Task.async(fn ->
        Command.apply_fun(command, name)
      end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:suspend, reason, metadata}} ->
        {:suspend, reason, metadata, command}

      {:ok, %{} = result} ->
        {:ok, result}

      {:ok, other} ->
        {:error, {:invalid_stage_result, other}}

      nil ->
        {:error, :timeout}

      {:exit, reason} ->
        {:error, reason}
    end
  end
end
