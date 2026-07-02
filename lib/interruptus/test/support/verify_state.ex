defmodule Interruptus.Test.Support.VerifyState do
  @moduledoc false

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> :not_done end, name: __MODULE__)
  end

  def set(value) do
    ensure_started()
    Agent.update(__MODULE__, fn _ -> value end)
  end

  def get do
    ensure_started()
    Agent.get(__MODULE__, & &1)
  end

  def reset! do
    set(:not_done)
  end

  defp ensure_started do
    unless Process.whereis(__MODULE__) do
      {:ok, _} = start_link()
    end
  end
end
