defmodule Interruptus.EngineTest do
  use ExUnit.Case, async: true

  alias Interruptus.Engine
  alias Interruptus.Test.Support.Workflows.Simple

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    :ok
  end

  test "run_segment/4 runs checkpoint segment" do
    command = Simple.new(%{value: 2})
    [stage, checkpoint] = Simple.flattened_pipelines()

    assert {:ok, updated} = Engine.run_segment(Simple, stage, command)
    assert updated.data.result == 4

    assert {:ok, completed} = Engine.run_segment(Simple, checkpoint, updated)
    assert completed.data.result == 4
    assert Process.get(:last_saved) == 4
  end

  test "run_from/4 completes all segments" do
    command = Simple.new(%{value: 5})
    assert {:completed, result} = Engine.run_from(Simple, command, 0)
    assert result.success
    assert result.data.result == 10
  end
end
