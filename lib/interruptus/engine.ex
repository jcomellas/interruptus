defmodule Interruptus.Engine do
  @moduledoc """
  Pure in-memory execution engine for workflow segments.

  Handles verify-then-run logic for checkpoint segments and individual stages.
  Used by `Interruptus.Runner` for durable execution and by workflow `run/1`
  for in-memory testing without persistence.

  Checkpoint segments optionally call a `verify/1` function before running pipelines.
  Verify must return `:done`, `:not_done`, or `:failed` and must be idempotent.

  ## Failure containment

  Exceptions, throws, and exits raised by stage or verify functions are caught
  and returned as `{:error, reason, command}` tuples carrying the last good
  command. This lets `Interruptus.Runner` route crashes through the restart
  policy and lets compensation see in-segment mutations from earlier pipelines.
  """

  alias Interruptus.Command

  @type segment :: Interruptus.Workflow.Segment.t()

  @doc """
  Runs a single segment (verify + pipelines) against a command struct.

  For checkpoint segments with a `verify` function, verify runs first (subject
  to the same `:timeout` as stages):

    * `:done` — skip pipelines, return `{:ok, command}`
    * `:not_done` — run pipelines
    * `:failed` — return `{:error, :verify_failed, command}`

  ## Options

    * `:timeout` - per-stage and verify timeout in ms, or `:infinity` (default)

  ## Returns

    * `{:ok, command}` - segment completed successfully
    * `{:suspend, reason, metadata, command}` - stage returned suspend
    * `{:halted, command}` - stage called `halt/2` or set `halted: true`
    * `{:error, reason, command}` - failure with last good command
  """
  @spec run_segment(module(), segment(), Command.t(), keyword()) ::
          {:ok, Command.t()}
          | {:suspend, term(), map(), Command.t()}
          | {:halted, Command.t()}
          | {:error, term(), Command.t()}
  def run_segment(workflow_module, segment, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, :infinity)

    case maybe_verify(workflow_module, segment, command, timeout) do
      {:skip, command} ->
        {:ok, command}

      {:run, command} ->
        run_pipelines(workflow_module, segment.pipelines, command, timeout)

      {:error, reason, command} ->
        {:error, reason, command}
    end
  end

  @doc """
  Runs consecutive segments from `from_index` until the next durability boundary.

  Bare `:stage` segments run in-process without returning to the caller. The
  span stops after a successful `:checkpoint` segment, on suspend/halt/error,
  or when the pipeline ends (last segment was a bare stage).

  Used by `Interruptus.Runner` so one supervised task covers an entire attempt
  span (pure stages plus the following checkpoint).

  ## Returns

    * `{:ok, command, end_index}` — completed through `end_index` (checkpoint
      or final bare stage); caller persists or completes
    * `{:suspend, reason, metadata, command, end_index}` — suspended at index
    * `{:halted, command, end_index}` — halted at index
    * `{:error, reason, command}` — failure (last-good command)
  """
  @spec run_span(module(), [segment()], non_neg_integer(), Command.t(), keyword()) ::
          {:ok, Command.t(), non_neg_integer()}
          | {:suspend, term(), map(), Command.t(), non_neg_integer()}
          | {:halted, Command.t(), non_neg_integer()}
          | {:error, term(), Command.t()}
  def run_span(workflow_module, segments, from_index, command, opts \\ [])
      when is_list(segments) and is_integer(from_index) and from_index >= 0 do
    do_run_span(workflow_module, segments, from_index, command, opts)
  end

  @doc """
  Runs all segments from the given index until completion, suspend, halt, or error.
  """
  @spec run_from(module(), Command.t(), non_neg_integer(), keyword()) ::
          {:completed, Command.t()}
          | {:suspend, term(), map(), Command.t(), non_neg_integer()}
          | {:halted, Command.t(), non_neg_integer()}
          | {:error, term(), Command.t()}
  def run_from(workflow_module, command, from_index, opts \\ []) do
    segments = workflow_module.flattened_pipelines()
    do_run_from(workflow_module, command, segments, from_index, opts)
  end

  @spec do_run_span(module(), [segment()], non_neg_integer(), Command.t(), keyword()) ::
          {:ok, Command.t(), non_neg_integer()}
          | {:suspend, term(), map(), Command.t(), non_neg_integer()}
          | {:halted, Command.t(), non_neg_integer()}
          | {:error, term(), Command.t()}
  defp do_run_span(_workflow_module, segments, index, command, _opts)
       when index >= length(segments) do
    # Caller should not start a span past the end; treat as empty success at
    # the last valid index so the runner can complete.
    {:ok, Command.maybe_mark_successful(command), max(index - 1, 0)}
  end

  defp do_run_span(workflow_module, segments, index, command, opts) do
    segment = Enum.at(segments, index)

    case run_segment(workflow_module, segment, command, opts) do
      {:ok, updated} ->
        cond do
          segment.type == :checkpoint ->
            {:ok, updated, index}

          index + 1 >= length(segments) ->
            {:ok, updated, index}

          true ->
            do_run_span(workflow_module, segments, index + 1, updated, opts)
        end

      {:suspend, reason, metadata, updated} ->
        {:suspend, reason, metadata, updated, index}

      {:halted, halted} ->
        {:halted, halted, index}

      {:error, reason, failed_command} ->
        {:error, reason, failed_command}
    end
  end

  @spec do_run_from(module(), Command.t(), [segment()], non_neg_integer(), keyword()) ::
          {:completed, Command.t()}
          | {:suspend, term(), map(), Command.t(), non_neg_integer()}
          | {:halted, Command.t(), non_neg_integer()}
          | {:error, term(), Command.t()}
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

      {:error, reason, failed_command} ->
        {:error, reason, failed_command}
    end
  end

  @spec maybe_verify(module(), segment(), Command.t(), :infinity | pos_integer()) ::
          {:skip, Command.t()} | {:run, Command.t()} | {:error, term(), Command.t()}
  defp maybe_verify(_workflow_module, %{verify: nil}, command, _timeout), do: {:run, command}

  defp maybe_verify(workflow_module, %{verify: verify}, command, timeout) do
    fun = fn -> verify_result(workflow_module, verify, command) end

    case run_with_timeout(fun, timeout) do
      :done ->
        {:skip, command}

      :not_done ->
        {:run, command}

      {:error, reason} ->
        {:error, reason, command}

      other ->
        {:error, {:invalid_verify_result, other}, command}
    end
  end

  @spec verify_result(module(), atom(), Command.t()) :: :done | :not_done | {:error, term()}
  defp verify_result(workflow_module, verify, command) do
    case apply(workflow_module, verify, [command]) do
      :done -> :done
      :not_done -> :not_done
      :failed -> {:error, :verify_failed}
      other -> {:error, {:invalid_verify_result, other}}
    end
  rescue
    exception ->
      {:error, {:exception, exception, __STACKTRACE__}}
  catch
    :throw, value -> {:error, {:throw, value}}
    :exit, reason -> {:error, {:exit, reason}}
  end

  @spec run_pipelines(module(), [atom()], Command.t(), :infinity | pos_integer()) ::
          {:ok, Command.t()}
          | {:suspend, term(), map(), Command.t()}
          | {:halted, Command.t()}
          | {:error, term(), Command.t()}
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

      {:error, reason, failed_command} ->
        {:error, reason, failed_command}
    end
  end

  @spec run_stage(module(), atom(), Command.t(), :infinity | pos_integer()) ::
          {:ok, Command.t()}
          | {:suspend, term(), map(), Command.t()}
          | {:error, term(), Command.t()}
  defp run_stage(_workflow_module, name, command, :infinity) do
    safe_apply(command, name)
  end

  defp run_stage(_workflow_module, name, command, timeout) when is_integer(timeout) do
    case run_with_timeout(fn -> safe_apply(command, name) end, timeout) do
      {:ok, updated} ->
        {:ok, updated}

      {:suspend, reason, metadata, updated} ->
        {:suspend, reason, metadata, updated}

      {:error, reason, failed_command} ->
        {:error, reason, failed_command}

      {:error, reason} ->
        {:error, reason, command}
    end
  end

  @spec safe_apply(Command.t(), atom()) ::
          {:ok, Command.t()}
          | {:suspend, term(), map(), Command.t()}
          | {:error, term(), Command.t()}
  defp safe_apply(command, name) do
    case Command.apply_fun(command, name) do
      {:suspend, reason, metadata, updated} ->
        {:suspend, reason, metadata, updated}

      {:suspend, reason, metadata} ->
        {:suspend, reason, metadata, command}

      {:error, reason, %{} = failed_command} ->
        {:error, reason, failed_command}

      {:error, reason} ->
        {:error, reason, command}

      %{} = result ->
        {:ok, result}

      other ->
        {:error, {:invalid_stage_result, other}, command}
    end
  rescue
    exception ->
      {:error, {:exception, exception, __STACKTRACE__}, command}
  catch
    :throw, value -> {:error, {:throw, value}, command}
    :exit, reason -> {:error, {:exit, reason}, command}
  end

  @spec run_with_timeout((-> term()), :infinity | pos_integer()) :: term()
  defp run_with_timeout(fun, :infinity), do: fun.()

  defp run_with_timeout(fun, timeout) when is_integer(timeout) do
    task = Task.async(fun)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} ->
        result

      nil ->
        {:error, :timeout}

      {:exit, reason} ->
        {:error, {:exit, reason}}
    end
  end
end
