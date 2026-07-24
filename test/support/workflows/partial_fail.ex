defmodule Interruptus.Test.Support.Workflows.PartialFail do
  @moduledoc false

  # First checkpoint completes (so compensation is in scope). Second checkpoint
  # mutates data then raises; compensation must see the mutated command and
  # run the in-flight undo_mutate before undo_setup.

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :id, :string

    data :seen, :string
    data :comp_seen, :string

    checkpoint compensate: :undo_setup do
      verify :verify_setup
      pipeline :setup
    end

    checkpoint compensate: :undo_mutate do
      verify :verify_mutate
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

  def verify_setup(command) do
    if command.data.seen in ["setup", "from-mutate"], do: :done, else: :not_done
  end

  def verify_mutate(command) do
    if command.data.seen == "from-mutate", do: :done, else: :not_done
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
