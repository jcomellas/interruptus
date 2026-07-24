defmodule Interruptus.Test.Support.Workflows.NoCompensate do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :id, :string

    pipeline :fail_me

    restart_policy max_attempts: 1, backoff: :constant, base_interval: 10
  end

  def fail_me(command, _params, _data) do
    Command.halt(command)
  end
end
