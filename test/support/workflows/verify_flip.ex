defmodule Interruptus.Test.Support.Workflows.VerifyFlip do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.InvocationCounter

  workflow do
    param :value, :integer

    data :result, :integer

    pipeline :prepare

    checkpoint do
      verify :verify_checkpoint
      pipeline :apply_result
    end

    # Crash-recovery reclaims consume attempts (durable pre-execution attempt
    # accounting), so allow enough budget for the interruption tests.
    restart_policy max_attempts: 3, backoff: :constant, base_interval: 10
    rollback_policy compensate: [:undo]
  end

  def prepare(command, %{value: value}, _data) do
    Command.put_data(command, :result, value)
  end

  def verify_checkpoint(_command) do
    Interruptus.Test.Support.VerifyState.get()
  end

  def apply_result(command, _params, _data) do
    Interruptus.Test.Support.Barrier.hold(:before_apply)
    :ok = Interruptus.Test.Support.Barrier.await(:before_apply)
    InvocationCounter.increment(:apply_result)
    command
  end

  def undo(command), do: command
end
