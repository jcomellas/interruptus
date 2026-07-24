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

  test "checkpoint names default to verify or first pipeline" do
    [stage, checkpoint] = Simple.flattened_pipelines()
    assert stage.name == :double
    assert checkpoint.name == :verify_doubled

    alias Interruptus.Test.Support.Workflows.SpanStages

    names = Enum.map(SpanStages.flattened_pipelines(), & &1.name)
    assert names == [:stage_a, :stage_b, :stage_c]
  end

  test "Interruptus.segment_name/2 resolves module indexes" do
    assert {:ok, :double} = Interruptus.segment_name(Simple, 0)
    assert {:ok, :verify_doubled} = Interruptus.segment_name(Simple, 1)
    assert {:ok, nil} = Interruptus.segment_name(Simple, 99)
  end

  test "explicit checkpoint names are used and appear in the fingerprint" do
    alias Interruptus.Test.Support.Workflows.NamedCheckpoints

    [stage, checkpoint] = NamedCheckpoints.flattened_pipelines()
    assert stage.name == :prepare
    assert checkpoint.name == :debit

    assert is_binary(NamedCheckpoints.pipeline_fingerprint())
    assert NamedCheckpoints.pipeline_fingerprint() != Simple.pipeline_fingerprint()
  end

  test "duplicate segment names raise at compile time" do
    assert_raise CompileError, ~r/duplicate workflow segment names/, fn ->
      Code.compile_string("""
      defmodule Interruptus.Test.Support.Workflows.DupNames#{System.unique_integer([:positive])} do
        use Interruptus.Workflow

        workflow do
          checkpoint :same do
            pipeline :a
          end

          checkpoint :same do
            pipeline :b
          end
        end

        def a(cmd, _, _), do: cmd
        def b(cmd, _, _), do: cmd
      end
      """)
    end
  end
end
