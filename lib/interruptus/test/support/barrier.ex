defmodule Interruptus.Test.Support.Barrier do
  @moduledoc false

  use GenServer

  @type gate :: atom()

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def hold(name) when is_atom(name) do
    ensure_started()
    GenServer.call(__MODULE__, {:hold, name})
  end

  def await(name, timeout \\ 5_000) when is_atom(name) do
    ensure_started()
    GenServer.call(__MODULE__, {:await, name, self()}, timeout + 100)
  catch
    :exit, {:timeout, _} ->
      :timeout
  end

  def release(name) when is_atom(name) do
    ensure_started()
    GenServer.call(__MODULE__, {:release, name})
  end

  def waiting?(name) when is_atom(name) do
    ensure_started()
    GenServer.call(__MODULE__, {:waiting?, name})
  end

  def await_held(name, timeout \\ 5_000) when is_atom(name) do
    ensure_started()
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_held(name, deadline)
  end

  def reset! do
    ensure_started()
    GenServer.call(__MODULE__, :reset)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:hold, name}, _from, state) do
    {:reply, :ok, Map.put(state, name, :held)}
  end

  def handle_call({:release, name}, _from, state) do
    waiters = Map.get(state, {:waiters, name}, [])

    Enum.each(waiters, fn from ->
      GenServer.reply(from, :ok)
    end)

    state =
      state
      |> Map.put(name, :released)
      |> Map.delete({:waiters, name})

    {:reply, :ok, state}
  end

  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %{}}
  end

  def handle_call({:await, name, _waiter}, from, state) do
    case Map.get(state, name) do
      :held ->
        waiters = Map.get(state, {:waiters, name}, [])
        {:noreply, put_in(state, [{:waiters, name}], [from | waiters])}

      :released ->
        {:reply, :ok, state}

      _ ->
        {:reply, :ok, state}
    end
  end

  def handle_call({:waiting?, name}, _from, state) do
    waiters = Map.get(state, {:waiters, name}, [])
    {:reply, waiters != [], state}
  end

  defp do_await_held(name, deadline) do
    if waiting?(name) do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        {:error, :timeout}
      else
        Process.sleep(50)
        do_await_held(name, deadline)
      end
    end
  end

  defp ensure_started do
    unless Process.whereis(__MODULE__) do
      {:ok, _} = start_link()
    end
  end
end
