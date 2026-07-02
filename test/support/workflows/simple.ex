defmodule Interruptus.Test.Support.Workflows.Simple do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    param(:value, :integer)

    data(:result, :integer)

    pipeline(:double)

    checkpoint do
      verify(:verify_doubled)
      pipeline(:save_result)
    end

    restart_policy(max_attempts: 2, backoff: :constant, base_interval: 10)
    rollback_policy(compensate: [:undo])
  end

  def double(command, %{value: value}, _data) do
    Command.put_data(command, :result, value * 2)
  end

  def save_result(command, _params, %{result: result}) do
    Process.put(:last_saved, result)
    command
  end

  def verify_doubled(_command) do
    case Process.get(:verify_result, :not_done) do
      :done -> :done
      :not_done -> :not_done
      :failed -> :failed
    end
  end

  def undo(command) do
    Process.delete(:last_saved)
    command
  end
end
