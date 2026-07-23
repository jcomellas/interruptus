defmodule Interruptus.WorkflowTest do
  use ExUnit.Case, async: false

  alias Interruptus.Test.Support.Workflows.Simple

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    :ok
  end

  test "run/1 executes pipeline in memory" do
    result = Simple.run(%{value: 3})
    assert result.success
    assert result.data.result == 6
    assert Process.get(:last_saved) == 6
  end

  test "verify :done skips checkpoint stages" do
    Process.put(:verify_result, :done)

    result = Simple.run(%{value: 3})
    assert result.success
    refute Process.get(:last_saved)
  end

  test "verify :not_done re-runs checkpoint stages" do
    Process.put(:verify_result, :not_done)

    result = Simple.run(%{value: 3})
    assert result.success
    assert Process.get(:last_saved) == 6
  end

  test "verify :failed returns error" do
    Process.put(:verify_result, :failed)

    assert {:error, :verify_failed, _command} = Simple.run(%{value: 3})
  end

  test "segments and policies are defined" do
    assert length(Simple.segments()) == 2
    assert Simple.restart_policy().max_attempts == 2
    assert Simple.rollback_policy().compensate == [:undo]
  end

  test "stage_timeout defaults to :infinity and is configurable" do
    assert Simple.stage_timeout() == :infinity
    assert Interruptus.Test.Support.Workflows.TimedOut.stage_timeout() == 100
  end

  test "checkpoint compensate option is captured in flattened segments" do
    alias Interruptus.Test.Support.Workflows.CompCrash

    segments = CompCrash.flattened_pipelines()

    assert Enum.map(segments, & &1.compensate) == [:undo_one, :undo_two, nil, :undo_never]
    assert Enum.all?(segments, &(&1.type == :checkpoint))

    # Plain checkpoints and stages carry compensate: nil.
    assert Enum.map(Simple.flattened_pipelines(), & &1.compensate) == [nil, nil]
  end
end
