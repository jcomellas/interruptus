defmodule Interruptus.Test.Support.Workflows.Barrier do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.Barrier

  workflow do
    param :token, :string

    data :step, :integer

    pipeline :step_one

    checkpoint do
      pipeline :gated_stage
    end
  end

  def step_one(command, _params, _data) do
    Command.put_data(command, :step, 1)
  end

  def gated_stage(command, _params, _data) do
    Barrier.hold(:before_checkpoint)
    :ok = Barrier.await(:before_checkpoint)
    Command.put_data(command, :step, 2)
  end
end
