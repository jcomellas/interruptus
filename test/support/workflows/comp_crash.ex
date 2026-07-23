defmodule Interruptus.Test.Support.Workflows.CompCrash do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.Barrier
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :token, :string

    data :phase, :integer

    checkpoint compensate: :undo_one do
      pipeline :step_a
    end

    checkpoint compensate: :undo_two do
      pipeline :step_b
    end

    checkpoint do
      pipeline :always_halts
    end

    checkpoint compensate: :undo_never do
      pipeline :unreached
    end

    restart_policy max_attempts: 3, backoff: :constant, base_interval: 10
  end

  def step_a(command, _params, _data), do: Command.put_data(command, :phase, 1)
  def step_b(command, _params, _data), do: Command.put_data(command, :phase, 2)

  def always_halts(command, _params, _data), do: Command.halt(command)

  def unreached(command, _params, _data), do: command

  # LIFO: undo_two runs first. It gates on a barrier so tests can crash the
  # runner mid-compensation.
  def undo_two(command) do
    Barrier.hold(:in_undo_two)
    :ok = Barrier.await(:in_undo_two)
    CompensateOrder.record(:undo_two)
    command
  end

  def undo_one(command) do
    CompensateOrder.record(:undo_one)
    command
  end

  def undo_never(command) do
    CompensateOrder.record(:undo_never)
    command
  end
end
