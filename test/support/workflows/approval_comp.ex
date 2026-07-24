defmodule Interruptus.Test.Support.Workflows.ApprovalComp do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :token, :string

    data :step, :integer

    checkpoint compensate: :undo_reserve do
      verify :verify_reserve
      pipeline :reserve
    end

    checkpoint do
      pipeline :await_gate
    end
  end

  def reserve(command, _params, _data) do
    Command.put_data(command, :step, 1)
  end

  def await_gate(command, _params, _data) do
    if Interruptus.Test.Support.ApprovalState.approved?(command.params.token) do
      Command.put_data(command, :step, 2)
    else
      {:suspend, :await_approval, %{token: command.params.token}}
    end
  end

  def verify_reserve(command) do
    if command.data.step >= 1, do: :done, else: :not_done
  end

  def undo_reserve(command) do
    CompensateOrder.record(:undo_reserve)
    command
  end
end
