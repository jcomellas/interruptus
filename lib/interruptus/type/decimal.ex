defmodule Interruptus.Type.Decimal do
  @moduledoc """
  Ecto type for decimals persisted as normalized strings in JSONB.

  Used by workflow `param` and `data` fields declared with `:decimal`.
  """

  use Ecto.Type

  @type t :: Decimal.t()

  @impl true
  def type, do: :string

  @impl true
  def cast(value) when is_binary(value) do
    case Decimal.parse(value) do
      {decimal, ""} -> {:ok, decimal}
      _ -> :error
    end
  end

  def cast(value) when is_integer(value), do: {:ok, Decimal.new(value)}
  def cast(value) when is_float(value), do: value |> Decimal.from_float() |> then(&{:ok, &1})
  def cast(%Decimal{} = value), do: {:ok, value}
  def cast(_), do: :error

  @impl true
  def load(value) when is_binary(value), do: cast(value)
  def load(_), do: :error

  @impl true
  def dump(%Decimal{} = value), do: {:ok, Decimal.to_string(value, :normal)}
  def dump(_), do: :error
end
