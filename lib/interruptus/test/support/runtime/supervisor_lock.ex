defmodule Interruptus.Test.Support.Runtime.SupervisorLock do
  @moduledoc false

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, [], Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec with_lock((-> term()), timeout()) :: term()
  def with_lock(fun, timeout \\ :infinity) when is_function(fun, 0) do
    GenServer.call(__MODULE__, {:with_lock, fun}, timeout)
  end

  @impl true
  def init([]), do: {:ok, %{}}

  @impl true
  def handle_call({:with_lock, fun}, _from, state) when is_function(fun, 0) do
    {:reply, fun.(), state}
  end
end
