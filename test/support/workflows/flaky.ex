defmodule Interruptus.Test.Support.Workflows.Flaky do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.InvocationCounter

  workflow do
    param :succeed_on_attempt, :integer

    data :result, :integer

    checkpoint do
      pipeline :flaky_stage
    end

    restart_policy max_attempts: 5, backoff: :constant, base_interval: 10
    rollback_policy compensate: [:undo]
  end

  def flaky_stage(command, %{succeed_on_attempt: succeed_on}, _data) do
    InvocationCounter.increment(:flaky_stage)

    if InvocationCounter.count(:flaky_stage) >= succeed_on do
      Command.put_data(command, :result, 42)
    else
      Command.halt(command)
    end
  end

  def undo(command) do
    Interruptus.Test.Support.CompensateOrder.record(:flaky_undo)
    command
  end
end
