defmodule Interruptus.Test.Support.Workflows.SuspendMutate do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :token, :string

    data :note, :string

    pipeline :mutate_then_suspend
  end

  def mutate_then_suspend(command, _params, _data) do
    updated = Command.put_data(command, :note, "kept")
    Command.suspend(updated, :await, %{token: command.params.token})
  end
end
