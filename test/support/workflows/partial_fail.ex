defmodule Interruptus.Test.Support.Workflows.PartialFail do
  @moduledoc false

  # First checkpoint completes (so compensation is in scope). Second checkpoint
  # mutates data then raises; compensation must see the mutated command.

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :id, :string

    data :seen, :string
    data :comp_seen, :string

    checkpoint compensate: :undo_setup do
      pipeline :setup
    end

    checkpoint compensate: :undo_mutate do
      pipeline :mutate
      pipeline :boom
    end

    restart_policy max_attempts: 1, backoff: :constant, base_interval: 10
  end

  def setup(command, _params, _data) do
    Command.put_data(command, :seen, "setup")
  end

  def mutate(command, _params, _data) do
    Command.put_data(command, :seen, "from-mutate")
  end

  def boom(_command, _params, _data) do
    raise "partial fail boom"
  end

  def undo_setup(command) do
    CompensateOrder.record({:undo_setup, command.data.seen})
    command
  end

  def undo_mutate(command) do
    CompensateOrder.record({:undo_mutate, command.data.seen})
    Command.put_data(command, :comp_seen, command.data.seen)
  end
end
