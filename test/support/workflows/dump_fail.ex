defmodule Interruptus.Test.Support.Workflows.DumpFail do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :value, :integer
    data :label, :string

    checkpoint do
      pipeline :set_bad_data
    end
  end

  def set_bad_data(command, _params, _data) do
    Command.put_data(command, :label, 123)
  end
end
