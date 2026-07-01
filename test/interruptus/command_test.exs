defmodule Interruptus.CommandTest do
  use ExUnit.Case, async: true

  alias Interruptus.Command

  defmodule Sample do
    defstruct [:params, :data, :errors, :halted, :success]

    def new(params),
      do: %__MODULE__{params: params, data: %{}, errors: %{}, halted: false, success: false}
  end

  test "put_data/3 sets data field" do
    command = Sample.new(%{a: 1})
    assert %{data: %{x: 2}} = Command.put_data(command, :x, 2)
  end

  test "halt/1 marks command as halted" do
    command = Sample.new(%{})
    assert %{halted: true, success: false} = Command.halt(command)
    assert %{halted: true, success: true} = Command.halt(command, success: true)
  end

  test "maybe_mark_successful/1" do
    command = Sample.new(%{})
    assert %{success: true} = Command.maybe_mark_successful(command)
    assert %{success: false} = command |> Command.halt() |> Command.maybe_mark_successful()
  end
end
