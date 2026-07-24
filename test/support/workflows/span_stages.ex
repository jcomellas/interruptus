defmodule Interruptus.Test.Support.Workflows.SpanStages do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :value, :integer

    data :a, :integer
    data :b, :integer
    data :c, :integer

    pipeline :stage_a
    pipeline :stage_b

    checkpoint do
      pipeline :stage_c
    end
  end

  def stage_a(command, %{value: value}, _data) do
    Command.put_data(command, :a, value)
  end

  def stage_b(command, _params, %{a: a}) do
    Command.put_data(command, :b, a + 1)
  end

  def stage_c(command, _params, %{b: b}) do
    Command.put_data(command, :c, b + 1)
  end
end
