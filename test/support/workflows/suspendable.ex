defmodule Interruptus.Test.Support.Workflows.Suspendable do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param(:token)

    data(:step)

    pipeline(:step_one)

    checkpoint do
      pipeline(:step_two)
    end
  end

  def step_one(command, _params, _data) do
    Command.put_data(command, :step, 1)
  end

  def step_two(command, _params, _data) do
    if Interruptus.Test.Support.ApprovalState.approved?(command.params.token) do
      Command.put_data(command, :step, 2)
    else
      {:suspend, :await_approval, %{token: command.params.token}}
    end
  end
end
