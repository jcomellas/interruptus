defmodule Interruptus.Test.Support.Runtime do
  @moduledoc false

  alias Interruptus.Test.Support.Runtime.SupervisorLock

  @otp_children [
    Interruptus.Registry,
    Interruptus.RunnerSupervisor,
    Interruptus.Recovery
  ]

  @doc """
  Ensures the `:interruptus` application and its OTP children are running.

  Serializes access with `SupervisorLock` so parallel test modules cannot
  race on shared Registry / supervisor processes. Repairs the OTP tree when
  the application is marked started but children have exited.
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
    case Application.ensure_all_started(:interruptus) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      {:error, reason} -> raise_otp_error(reason)
    end

    if otp_healthy?() do
      :ok
    else
      restart_application!()
    end
  end

  @spec restart_application!() :: :ok
  defp restart_application! do
    if application_started?(:interruptus) do
      :ok = Application.stop(:interruptus)
      wait_for_application_stop(:interruptus)
    end

    case Application.ensure_all_started(:interruptus) do
      {:ok, _} -> verify_healthy!()
      {:error, {:already_started, _}} -> verify_healthy!()
      {:error, reason} -> raise_otp_error(reason)
    end
  end

  @spec verify_healthy!() :: :ok
  defp verify_healthy! do
    if otp_healthy?() do
      :ok
    else
      missing = Enum.reject(@otp_children, &Process.whereis/1)

      raise "Interruptus OTP children are not running after restart: #{inspect(missing)}"
    end
  end

  @spec otp_healthy?() :: boolean()
  defp otp_healthy? do
    Enum.all?(@otp_children, &Process.whereis/1)
  end

  @spec application_started?(atom()) :: boolean()
  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _, _} ->
      started_app == app
    end)
  end

  @spec wait_for_application_stop(atom()) :: :ok
  defp wait_for_application_stop(app) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    do_wait_for_application_stop(app, deadline)
  end

  @spec do_wait_for_application_stop(atom(), integer()) :: :ok
  defp do_wait_for_application_stop(app, deadline) do
    if application_started?(app) and System.monotonic_time(:millisecond) < deadline do
      Process.sleep(10)
      do_wait_for_application_stop(app, deadline)
    else
      :ok
    end
  end

  @spec raise_otp_error(term()) :: no_return()
  defp raise_otp_error(reason) do
    raise "failed to start :interruptus application: #{inspect(reason)}"
  end

  @spec do_cleanup!() :: :ok
  defp do_cleanup! do
    if Process.whereis(Interruptus.RunnerSupervisor) do
      stop_all_runners()
      wait_for_runners(System.monotonic_time(:millisecond) + 2_000)
    end

    :ok
  end

  @spec stop_all_runners() :: :ok
  defp stop_all_runners do
    Interruptus.RunnerSupervisor
    |> DynamicSupervisor.which_children()
    |> Enum.each(fn
      {_, pid, _, _} when is_pid(pid) ->
        _ = DynamicSupervisor.terminate_child(Interruptus.RunnerSupervisor, pid)

      _ ->
        :ok
    end)

    :ok
  end

  @spec wait_for_runners(integer()) :: :ok
  defp wait_for_runners(deadline) do
    if runner_children_empty?() or System.monotonic_time(:millisecond) >= deadline do
      :ok
    else
      Process.sleep(20)
      wait_for_runners(deadline)
    end
  end

  @spec runner_children_empty?() :: boolean()
  defp runner_children_empty? do
    case DynamicSupervisor.which_children(Interruptus.RunnerSupervisor) do
      [] -> true
      children -> Enum.all?(children, fn {_, pid, _, _} -> not is_pid(pid) end)
    end
  end
end
