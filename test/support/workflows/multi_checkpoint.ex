defmodule Interruptus.Test.Support.Workflows.MultiCheckpoint do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.Barrier

  workflow do
    param :value, :integer

    data :phase, :integer

    pipeline :phase_one

    checkpoint do
      pipeline :phase_two
    end

    checkpoint do
      pipeline :phase_three
    end
  end

  def phase_one(command, _params, _data) do
    Command.put_data(command, :phase, 1)
  end

  def phase_two(command, _params, _data) do
    Barrier.hold(:after_checkpoint_one)
    :ok = Barrier.await(:after_checkpoint_one)
    Command.put_data(command, :phase, 2)
  end

  def phase_three(command, _params, _data) do
    Command.put_data(command, :phase, 3)
  end
end
