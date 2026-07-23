defmodule Interruptus.Test.Support.Workflows.TypedFields do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    param :required_int, :integer
    param :optional_int, :integer, default: 10
    param :amount, :decimal

    data :name, :string
    data :count, :integer
    data :flag, :boolean
  end
end
