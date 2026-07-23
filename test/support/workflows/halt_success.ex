defmodule Interruptus.Test.Support.Workflows.HaltSuccess do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :id, :string

    data :done, :boolean

    pipeline :early_exit

    restart_policy max_attempts: 1
    rollback_policy compensate: [:should_not_run]
  end

  def early_exit(command, _params, _data) do
    command
    |> Command.put_data(:done, true)
    |> Command.halt(success: true)
  end

  def should_not_run(command) do
    CompensateOrder.record(:should_not_run)
    command
  end
end
