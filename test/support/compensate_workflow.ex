defmodule Interruptus.Test.Support.CompensateWorkflow do
  @moduledoc false

  use Interruptus.Workflow

  workflow do
    param(:id)
    data(:value)
  end

  def compensate_a(command) do
    Interruptus.Test.Support.CompensateOrder.record(:a)
    command
  end

  def compensate_b(command) do
    Interruptus.Test.Support.CompensateOrder.record(:b)
    command
  end
end

defmodule Interruptus.Test.Support.CompensateOrder do
  @moduledoc false

  use Agent

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> [] end, name: __MODULE__)
  end

  def reset! do
    ensure_started()
    Agent.update(__MODULE__, fn _ -> [] end)
  end

  def record(name) do
    ensure_started()
    Agent.update(__MODULE__, &[name | &1])
  end

  def all, do: Agent.get(__MODULE__, & &1)

  defp ensure_started do
    unless Process.whereis(__MODULE__) do
      {:ok, _} = start_link()
    end
  end
end
