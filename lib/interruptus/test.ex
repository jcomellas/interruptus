defmodule Interruptus.Test do
  @moduledoc """
  Test helpers for simulating interrupts and asserting workflow state.

  Use in integration tests with Ecto SQL Sandbox and a running Interruptus
  instance. Typical patterns:

      {:ok, %{id: id}} = Interruptus.start(MyWorkflow, params)
      assert {:ok, %{status: :completed}} = Interruptus.Test.await_status(id, :completed)

      :ok = Interruptus.Test.await_runner(id)
      :ok = Interruptus.Test.crash_runner(id)
      :ok = Interruptus.Test.expire_lease(config, id)
      :ok = Interruptus.Recovery.recover_all(config)
      assert {:ok, %{status: :completed}} = Interruptus.Test.await_status(id, :completed)
  """

  alias Interruptus.Claim
  alias Interruptus.Config
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  @doc """
  Polls the database until the workflow reaches the expected status.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance
    * `expected_status` - atom status to wait for (e.g. `:completed`, `:suspended`)

  ## Options

    * `:config` - Interruptus config (default `Interruptus.Config.fetch/0`)
    * `:timeout` - max wait in ms (default `5_000`)
    * `:interval` - poll interval in ms (default `50`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - instance with matching status
    * `{:error, :timeout}` - status not reached within timeout
  """
  @spec await_status(Ecto.UUID.t(), atom(), keyword()) ::
          {:ok, Interruptus.Schemas.WorkflowInstance.t()} | {:error, :timeout}
  def await_status(workflow_id, expected_status, opts \\ []) do
    config = Keyword.get(opts, :config, Config.fetch())
    timeout = Keyword.get(opts, :timeout, 5_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_status(config, workflow_id, expected_status, deadline, interval)
  end

  @spec do_await_status(
          Config.t(),
          Ecto.UUID.t(),
          atom(),
          integer(),
          non_neg_integer()
        ) :: {:ok, WorkflowInstance.t()} | {:error, :timeout}
  defp do_await_status(config, workflow_id, expected_status, deadline, interval) do
    case Store.get(config, workflow_id) do
      %WorkflowInstance{status: ^expected_status} = instance ->
        {:ok, instance}

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(interval)
          do_await_status(config, workflow_id, expected_status, deadline, interval)
        end
    end
  end

  @doc """
  Polls until a runner is registered for the workflow id.

  ## Options

    * `:timeout` - max wait in ms (default `5_000`)
    * `:interval` - poll interval in ms (default `50`)

  ## Returns

    * `:ok` - runner is registered
    * `{:error, :timeout}` - no runner within timeout
  """
  @spec await_runner(Ecto.UUID.t(), keyword()) :: :ok | {:error, :timeout}
  def await_runner(workflow_id, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_runner(workflow_id, deadline, interval)
  end

  @spec do_await_runner(Ecto.UUID.t(), integer(), non_neg_integer()) :: :ok | {:error, :timeout}
  defp do_await_runner(workflow_id, deadline, interval) do
    case runner_pid(workflow_id) do
      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(interval)
          do_await_runner(workflow_id, deadline, interval)
        end

      _pid ->
        :ok
    end
  end

  @doc """
  Returns the registered runner pid for a workflow, or `nil`.

  ## Options

    * `:config` - Interruptus config struct or name (default `Interruptus.Config.fetch/0`)
  """
  @spec runner_pid(Ecto.UUID.t(), keyword()) :: pid() | nil
  def runner_pid(workflow_id, opts \\ []) do
    config = resolve_config(opts)

    case Registry.lookup(Config.registry_name(config), workflow_id) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  @doc """
  Kills the runner process for a workflow to simulate a crash.

  After the lease expires, `Interruptus.Recovery` should reclaim and restart
  execution. Use with `expire_lease/2` and `await_status/3` to verify recovery.

  ## Options

    * `:config` - Interruptus config struct or name (default `Interruptus.Config.fetch/0`)

  ## Returns

    * `:ok` - runner was killed
    * `{:error, :not_running}` - no runner registered for this id
  """
  @spec crash_runner(Ecto.UUID.t(), keyword()) :: :ok | {:error, :not_running}
  def crash_runner(workflow_id, opts \\ []) do
    config = resolve_config(opts)

    case runner_pid(workflow_id, opts) do
      nil ->
        {:error, :not_running}

      pid ->
        _ =
          DynamicSupervisor.terminate_child(Config.runner_supervisor_name(config), pid)

        wait_for_runner_exit(workflow_id, opts)
    end
  end

  @spec wait_for_runner_exit(Ecto.UUID.t(), keyword()) :: :ok
  defp wait_for_runner_exit(workflow_id, opts) do
    deadline = System.monotonic_time(:millisecond) + 2_000

    if runner_pid(workflow_id, opts) == nil or
         System.monotonic_time(:millisecond) >= deadline do
      :ok
    else
      Process.sleep(20)
      wait_for_runner_exit(workflow_id, opts)
    end
  end

  @spec resolve_config(keyword()) :: Config.t()
  defp resolve_config(opts) do
    case Keyword.get(opts, :config) do
      %Config{} = config -> config
      nil -> Config.fetch()
      name when is_atom(name) -> Config.fetch(name)
    end
  end

  @doc """
  Sets `locked_until` to the past so the workflow becomes immediately reclaimable.

  Use after `crash_runner/1` because `:kill` does not invoke the runner's
  `terminate/2` callback and the lease may still be valid.

  ## Returns

    * `:ok` - lease expired
    * `{:error, term()}` - update failed
  """
  @spec expire_lease(Config.t(), Ecto.UUID.t()) :: :ok | {:error, term()}
  def expire_lease(config, workflow_id) do
    import Ecto.Query

    past = DateTime.add(DateTime.utc_now(), -60, :second)

    case Interruptus.Repo.update_all(
           config,
           from(w in WorkflowInstance, where: w.id == ^workflow_id),
           set: [locked_until: past]
         ) do
      {0, _} -> {:error, :not_found}
      {_, _} -> :ok
    end
  end

  @doc """
  Crashes any live runner, expires the lease, and runs recovery.
  """
  @spec recover_after_interrupt(Config.t(), Ecto.UUID.t()) :: :ok
  def recover_after_interrupt(config, workflow_id) do
    :ok = expire_lease(config, workflow_id)
    _ = crash_runner(workflow_id)

    Interruptus.Test.Support.Barrier.reset!()
    Interruptus.Recovery.recover_all(config)
    :ok
  end

  @doc """
  Returns a copy of the config with a different `node_id` for multi-node simulation.
  """
  @spec with_node_id(Config.t(), String.t()) :: Config.t()
  def with_node_id(%Config{} = config, node_id) do
    %{config | node_id: node_id}
  end

  @doc """
  Runs two concurrent `Claim.acquire/2` calls from different node configs.

  Requires the test process to own the SQL Sandbox. Each task is allowed
  sandbox access before acquiring.

  ## Returns

    A list of two results in task start order.
  """
  @spec race_acquire(Config.t(), Config.t(), Ecto.UUID.t()) :: [term()]
  def race_acquire(config_a, config_b, workflow_id) do
    parent = self()
    repo = Interruptus.Test.Repo

    task_a =
      Task.async(fn ->
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, self())
        Claim.acquire(config_a, workflow_id)
      end)

    task_b =
      Task.async(fn ->
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, self())
        Claim.acquire(config_b, workflow_id)
      end)

    [Task.await(task_a), Task.await(task_b)]
  end

  @doc """
  Asserts that a checkpoint row exists at the given stage index.

  ## Returns

    * `:ok` - checkpoint found
    * `{:error, :checkpoint_not_found}` - no matching checkpoint row
  """
  @spec assert_checkpoint(Ecto.UUID.t(), non_neg_integer(), keyword()) ::
          :ok | {:error, term()}
  def assert_checkpoint(workflow_id, stage_index, opts \\ []) do
    config = Keyword.get(opts, :config, Config.fetch())

    import Ecto.Query
    alias Interruptus.Repo
    alias Interruptus.Schemas.Checkpoint

    query =
      from(c in Checkpoint,
        where: c.workflow_id == ^workflow_id and c.stage_index == ^stage_index
      )

    case Repo.one(config, query) do
      nil -> {:error, :checkpoint_not_found}
      _ -> :ok
    end
  end

  @doc """
  Polls until a runner is blocked on a held barrier gate.
  """
  @spec await_barrier_held(atom(), keyword()) :: :ok | {:error, :timeout}
  def await_barrier_held(name, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    Interruptus.Test.Support.Barrier.await_held(name, timeout)
  end

  @doc """
  Asserts the invocation count for a stage in `InvocationCounter`.

  Raises if the count does not match.
  """
  @spec assert_invocations(atom(), non_neg_integer()) :: :ok
  def assert_invocations(stage, expected) do
    actual = Interruptus.Test.Support.InvocationCounter.count(stage)

    if actual == expected do
      :ok
    else
      raise "expected #{expected} invocations for #{inspect(stage)}, got #{actual}"
    end
  end
end
