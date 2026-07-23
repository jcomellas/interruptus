defmodule Interruptus.EngineTest do
  use ExUnit.Case, async: true

  alias Interruptus.Engine
  alias Interruptus.Test.Support.Workflows.Failing
  alias Interruptus.Test.Support.Workflows.InvalidVerify
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.Slow
  alias Interruptus.Test.Support.Workflows.Suspendable

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

  test "verify :failed returns error" do
    Process.put(:verify_result, :failed)
    command = Simple.new(%{value: 2})
    [_, checkpoint] = Simple.flattened_pipelines()

    assert {:error, :verify_failed, ^command} = Engine.run_segment(Simple, checkpoint, command)
  end

  test "invalid verify result returns error" do
    command = InvalidVerify.new()
    [checkpoint] = InvalidVerify.flattened_pipelines()

    assert {:error, {:invalid_verify_result, :bogus}, ^command} =
             Engine.run_segment(InvalidVerify, checkpoint, command)
  end

  test "suspend propagates from stage" do
    command = Suspendable.new(%{token: "t1"})
    [stage, checkpoint] = Suspendable.flattened_pipelines()

    assert {:ok, command} = Engine.run_segment(Suspendable, stage, command)

    assert {:suspend, :await_approval, %{token: "t1"}, updated} =
             Engine.run_segment(Suspendable, checkpoint, command)

    assert updated.data.step == 1
  end

  test "halt returns halted" do
    command = Failing.new(%{id: "1"})
    [_, checkpoint] = Failing.flattened_pipelines()

    assert {:halted, halted} = Engine.run_segment(Failing, checkpoint, command)
    assert halted.halted
  end

  test "run_from/4 resumes from intermediate index" do
    command = Simple.new(%{value: 3})
    [stage | _] = Simple.flattened_pipelines()

    assert {:ok, doubled} = Engine.run_segment(Simple, stage, command)
    assert {:completed, result} = Engine.run_from(Simple, doubled, 1)
    assert result.success
    assert result.data.result == 6
  end

  test "stage timeout returns error" do
    command = Slow.new()
    [stage] = Slow.flattened_pipelines()

    assert {:error, :timeout, ^command} = Engine.run_segment(Slow, stage, command, timeout: 50)
  end

  test "raised exceptions are contained as error tuples" do
    alias Interruptus.Test.Support.Workflows.Raising

    command = Raising.new(%{id: "e1"})
    [_first, boom_segment | _] = Raising.flattened_pipelines()

    assert {:error, {:exception, %RuntimeError{message: message}, stacktrace}, ^command} =
             Engine.run_segment(Raising, boom_segment, command)

    assert message =~ "boom stage always raises"
    assert is_list(stacktrace)

    # Same containment on the timeout execution path.
    assert {:error, {:exception, %RuntimeError{}, _}, ^command} =
             Engine.run_segment(Raising, boom_segment, command, timeout: 5_000)
  end

  test "invalid stage return values are errors, not crashes" do
    alias Interruptus.Test.Support.Workflows.BadReturn

    command = BadReturn.new()
    [segment] = BadReturn.flattened_pipelines()

    assert {:error, {:invalid_stage_result, :oops}, ^command} =
             Engine.run_segment(BadReturn, segment, command)

    assert {:error, {:invalid_stage_result, :oops}, ^command} =
             Engine.run_segment(BadReturn, segment, command, timeout: 5_000)
  end
end
