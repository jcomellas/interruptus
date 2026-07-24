defmodule Interruptus.Test.Support.Workflows.Raising do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.InvocationCounter

  workflow do
    param :id, :string

    data :step, :integer

    checkpoint compensate: :undo_first do
      verify :verify_first
      pipeline :first_step
    end

    checkpoint do
      pipeline :boom
    end

    restart_policy max_attempts: 2, backoff: :constant, base_interval: 10
    rollback_policy compensate: [:final_cleanup]
  end

  def first_step(command, _params, _data) do
    Command.put_data(command, :step, 1)
  end

  def boom(_command, _params, _data) do
    InvocationCounter.increment(:boom)
    raise "boom stage always raises"
  end

  def verify_first(command) do
    if command.data.step == 1, do: :done, else: :not_done
  end

  def undo_first(command) do
    CompensateOrder.record(:undo_first)
    command
  end

  def final_cleanup(command) do
    CompensateOrder.record(:final_cleanup)
    command
  end
end
