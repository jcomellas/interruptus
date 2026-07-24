defmodule Interruptus.Test.Support.Workflows.NamedCheckpoints do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :value, :integer

    data :result, :integer

    pipeline :prepare

    checkpoint :debit do
      verify :verify_debit
      pipeline :apply_debit
    end
  end

  def prepare(command, %{value: value}, _data) do
    Command.put_data(command, :result, value)
  end

  def verify_debit(_command), do: :not_done

  def apply_debit(command, _params, data) do
    Command.put_data(command, :result, data.result)
  end
end
