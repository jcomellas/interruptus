defmodule Interruptus.Test.Support.Workflows.BadReturn do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    checkpoint do
      pipeline :bad_stage
    end

    restart_policy max_attempts: 1, backoff: :constant, base_interval: 10
  end

  def bad_stage(_command, _params, _data), do: :oops
end
