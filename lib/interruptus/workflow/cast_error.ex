defmodule Interruptus.Workflow.CastError do
  @moduledoc """
  Structured error for workflow field load/dump failures.
  """

  defexception [:message, :field, :value, :operation, :reason]

  @type t :: %__MODULE__{
          message: String.t(),
          field: atom(),
          value: term(),
          operation: :load | :dump | :validate_dump,
          reason: term()
        }

  @impl true
  def exception(opts) do
    field = Keyword.fetch!(opts, :field)
    operation = Keyword.fetch!(opts, :operation)
    value = Keyword.get(opts, :value)
    reason = Keyword.get(opts, :reason, :invalid)

    %__MODULE__{
      field: field,
      value: value,
      operation: operation,
      reason: reason,
      message: "workflow field #{inspect(field)} #{operation} failed for value #{inspect(value)}: #{inspect(reason)}"
    }
  end

  @doc """
  Encodes a cast error for storage in `workflow_instance.errors`.
  """
  @spec encode(t()) :: map()
  def encode(%__MODULE__{field: field, value: value, operation: operation, reason: reason}) do
    %{
      "field" => Atom.to_string(field),
      "value" => inspect(value, limit: :infinity, printable_limit: :infinity),
      "operation" => Atom.to_string(operation),
      "reason" => inspect(reason, limit: :infinity, printable_limit: :infinity)
    }
  end
end
