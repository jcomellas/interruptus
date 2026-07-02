defmodule Interruptus.Test.Support.Workflows.Failing do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command
  alias Interruptus.Test.Support.CompensateOrder

  workflow do
    param :id, :string

    data :value, :integer

    pipeline :prepare

    checkpoint do
      verify :verify_checkpoint
      pipeline :maybe_halt
    end

    restart_policy max_attempts: 1, backoff: :constant, base_interval: 10
    rollback_policy compensate: [:compensate_step]
  end

  def prepare(command, _params, _data) do
    Command.put_data(command, :value, 1)
  end

  def verify_checkpoint(_command) do
    Interruptus.Test.Support.VerifyState.get()
  end

  def maybe_halt(command, _params, _data) do
    Command.halt(command)
  end

  def compensate_step(command) do
    CompensateOrder.record(:compensated)
    command
  end
end
