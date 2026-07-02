defmodule Interruptus.Test.Support.Workflows.Slow do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    pipeline :slow_stage
  end

  def slow_stage(command, _params, _data) do
    Process.sleep(200)
    command
  end
end
