defmodule Interruptus.Test.Support.Workflows.BarrierCompensate do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.Barrier
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :token, :string

    data :step, :integer

    checkpoint compensate: :undo_reserve do
      verify :verify_reserve
      pipeline :reserve
    end

    checkpoint do
      pipeline :gated
    end
  end

  def reserve(command, _params, _data) do
    Command.put_data(command, :step, 1)
  end

  def gated(command, _params, _data) do
    Barrier.hold(:before_cancel_comp)
    :ok = Barrier.await(:before_cancel_comp)
    Command.put_data(command, :step, 2)
  end

  def verify_reserve(command) do
    if command.data.step >= 1, do: :done, else: :not_done
  end

  def undo_reserve(command) do
    CompensateOrder.record(:undo_reserve)
    command
  end
end
