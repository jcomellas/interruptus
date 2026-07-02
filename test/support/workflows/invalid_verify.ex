defmodule Interruptus.Test.Support.Workflows.InvalidVerify do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    checkpoint do
      verify :verify_bad
      pipeline :noop
    end
  end

  def verify_bad(_command), do: :bogus

  def noop(command, _params, _data), do: command
end
