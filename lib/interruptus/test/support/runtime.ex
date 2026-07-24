defmodule Interruptus.Test.Support.Runtime do
  @moduledoc false

  alias Interruptus.Config
  alias Interruptus.Test.Support.Runtime.SupervisorLock

  # Options for the default test instance. Merged over config/test.exs app env
  # (repo, lease timings). Recovery scans are disabled: tests drive recovery
  # explicitly via Interruptus.Recovery.recover_all/1.
  @instance_opts [recovery_schedule: false]

  @doc """
  Ensures the default Interruptus instance supervision tree is running.

  Since the tree normally lives under a host application supervisor, tests
  start it detached (unlinked) so the calling test process's exit does not
  tear it down. Serializes access with `SupervisorLock` so parallel test
  modules cannot race on shared processes, and repairs the tree if children
  have exited.
  """
  @spec start!() :: :ok
  def start! do
    ensure_lock!()
    SupervisorLock.with_lock(&do_start!/0)
  end

  @doc """
  Stops all workflow runners before the SQL Sandbox owner exits.

  Runners hold sandbox checkouts; terminating them first avoids
  `DBConnection.ConnectionError` when the test process ends.
  """
  @spec cleanup!() :: :ok
  def cleanup! do
    ensure_lock!()
    SupervisorLock.with_lock(&do_cleanup!/0)
  end

  @spec ensure_lock!() :: :ok
  defp ensure_lock! do
    if Process.whereis(SupervisorLock) do
      :ok
    else
      {:ok, _} = SupervisorLock.start_link()
      :ok
    end
  end

  @spec do_start!() :: :ok
  defp do_start! do
    if tree_healthy?() do
      :ok
    else
      restart_tree!()
    end
  end

  @spec restart_tree!() :: :ok
  defp restart_tree! do
    sup_name = Config.supervisor_name(Interruptus)

    if pid = Process.whereis(sup_name) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        5_000 -> raise "Interruptus test supervision tree did not stop"
      end
    end

    case Interruptus.Supervisor.start_link(@instance_opts) do
      {:ok, pid} ->
        # The tree must survive the (transient) process that repaired it.
        Process.unlink(pid)
        verify_healthy!()

      {:error, {:already_started, _pid}} ->
        verify_healthy!()

      {:error, reason} ->
        raise "failed to start Interruptus test supervision tree: #{inspect(reason)}"
    end
  end

  @spec verify_healthy!() :: :ok
  defp verify_healthy! do
    if tree_healthy?() do
      :ok
    else
      missing = Enum.reject(tree_children(), &Process.whereis/1)

      raise "Interruptus tree children are not running after restart: #{inspect(missing)}"
    end
  end

  @spec tree_children() :: [atom()]
  defp tree_children do
    [
      Config.registry_name(Interruptus),
      Config.task_supervisor_name(Interruptus),
      Config.runner_supervisor_name(Interruptus),
      Config.recovery_name(Interruptus)
    ]
  end

  @spec tree_healthy?() :: boolean()
  defp tree_healthy? do
    Enum.all?([Config.supervisor_name(Interruptus) | tree_children()], &Process.whereis/1)
  end

  @spec do_cleanup!() :: :ok
  defp do_cleanup! do
    runner_sup = Config.runner_supervisor_name(Interruptus)

    if Process.whereis(runner_sup) do
      stop_all_runners(runner_sup)
      wait_for_runners(runner_sup, System.monotonic_time(:millisecond) + 2_000)
    end

    :ok
  end

  @spec stop_all_runners(atom()) :: :ok
  defp stop_all_runners(runner_sup) do
    runner_sup
    |> DynamicSupervisor.which_children()
    |> Enum.each(fn
      {_, pid, _, _} when is_pid(pid) ->
        _ = DynamicSupervisor.terminate_child(runner_sup, pid)

      _ ->
        :ok
    end)

    :ok
  end

  @spec wait_for_runners(atom(), integer()) :: :ok
  defp wait_for_runners(runner_sup, deadline) do
    if runner_children_empty?(runner_sup) or System.monotonic_time(:millisecond) >= deadline do
      :ok
    else
      Process.sleep(20)
      wait_for_runners(runner_sup, deadline)
    end
  end

  @spec runner_children_empty?(atom()) :: boolean()
  defp runner_children_empty?(runner_sup) do
    case DynamicSupervisor.which_children(runner_sup) do
      [] -> true
      children -> Enum.all?(children, fn {_, pid, _, _} -> not is_pid(pid) end)
    end
  end
end
