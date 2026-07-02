defmodule Interruptus.Test.Support.Workflows.Counting do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.InvocationCounter

  workflow do
    param :value, :integer

    data :result, :integer

    pipeline :prepare

    checkpoint do
      pipeline :side_effect
    end
  end

  def prepare(command, %{value: value}, _data) do
    Command.put_data(command, :result, value)
  end

  def side_effect(command, _params, _data) do
    InvocationCounter.increment(:side_effect)
    Interruptus.Test.Support.Barrier.hold(:in_side_effect)
    :ok = Interruptus.Test.Support.Barrier.await(:in_side_effect)
    command
  end
end
