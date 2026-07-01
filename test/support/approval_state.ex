defmodule Interruptus.Test.Support.ApprovalState do
  @moduledoc false

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> MapSet.new() end, name: __MODULE__)
  end

  def approve(token) do
    ensure_started()
    Agent.update(__MODULE__, &MapSet.put(&1, token))
  end

  def approved?(token) do
    ensure_started()
    Agent.get(__MODULE__, &MapSet.member?(&1, token))
  end

  def reset! do
    ensure_started()
    Agent.update(__MODULE__, fn _ -> MapSet.new() end)
  end

  defp ensure_started do
    unless Process.whereis(__MODULE__) do
      {:ok, _} = start_link()
    end
  end
end
