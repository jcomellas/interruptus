defmodule Interruptus.Test.Support.Workflows.LongSlow do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    data :done, :boolean

    checkpoint do
      pipeline :long_stage
    end
  end

  # Runs longer than the (test-shortened) lease so the heartbeat must keep
  # renewing while the stage executes.
  def long_stage(command, _params, _data) do
    Process.sleep(1_000)
    Command.put_data(command, :done, true)
  end
end
