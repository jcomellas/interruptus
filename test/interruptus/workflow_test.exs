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

    assert {:error, :verify_failed} = Simple.run(%{value: 3})
  end

  test "segments and policies are defined" do
    assert length(Simple.segments()) == 2
    assert Simple.restart_policy().max_attempts == 2
    assert Simple.rollback_policy().compensate == [:undo]
  end
end
