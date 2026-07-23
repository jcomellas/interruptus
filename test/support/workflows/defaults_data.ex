defmodule Interruptus.Test.Support.Workflows.DefaultsData do
  @moduledoc false

  use Interruptus.Workflow

  alias Interruptus.Command

  workflow do
    param :id, :string, default: "x"

    data :flag, :boolean, default: true
    data :label, :string, default: "hi"

    pipeline :echo
  end

  def echo(command, _params, data) do
    # Persist whatever defaults survived load/merge.
    command
    |> Command.put_data(:flag, data.flag)
    |> Command.put_data(:label, data.label)
  end
end
