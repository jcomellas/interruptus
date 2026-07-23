defmodule Interruptus.Test.Support.Workflows.SlowVerify do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    param :id, :string

    checkpoint do
      verify :hang
      pipeline :noop
    end

    stage_timeout(50)
    restart_policy max_attempts: 1, backoff: :constant, base_interval: 10
  end

  def hang(_command) do
    Process.sleep(5_000)
    :not_done
  end

  def noop(command, _params, _data), do: command
end
