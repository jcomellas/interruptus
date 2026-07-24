defmodule Interruptus.Test.Support.Workflows.WeirdSuspend do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    checkpoint do
      pipeline :suspend_with_tuple
    end
  end

  # Suspend reasons are not always strings or atoms; the runner must persist
  # a readable representation instead of crashing on String.Chars.
  def suspend_with_tuple(_command, _params, _data) do
    {:suspend, {:waiting_for, "partner-bank", 42}, %{}}
  end
end
