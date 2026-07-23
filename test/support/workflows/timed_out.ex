defmodule Interruptus.Test.Support.Workflows.TimedOut do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.InvocationCounter

  workflow do
    stage_timeout(100)

    data :done, :boolean

    checkpoint do
      pipeline :maybe_slow
    end

    restart_policy max_attempts: 3, backoff: :constant, base_interval: 10
  end

  # Hangs past stage_timeout on the first invocation; succeeds afterwards.
  def maybe_slow(command, _params, _data) do
    InvocationCounter.increment(:maybe_slow)

    if InvocationCounter.count(:maybe_slow) == 1 do
      Process.sleep(60_000)
    end

    Command.put_data(command, :done, true)
  end
end
