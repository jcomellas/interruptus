defmodule Interruptus.Test.Support.InvocationCounter do
  @moduledoc false

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def increment(stage) when is_atom(stage) do
    ensure_started()
    Agent.update(__MODULE__, &Map.update(&1, stage, 1, fn count -> count + 1 end))
  end

  def count(stage) when is_atom(stage) do
    ensure_started()
    Agent.get(__MODULE__, &Map.get(&1, stage, 0))
  end

  def reset! do
    ensure_started()
    Agent.update(__MODULE__, fn _ -> %{} end)
  end

  defp ensure_started do
    unless Process.whereis(__MODULE__) do
      {:ok, _} = start_link()
    end
  end
end
