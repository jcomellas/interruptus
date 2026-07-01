defmodule Interruptus.Test do
  @moduledoc """
  Test helpers for simulating interrupts and asserting workflow state.

  Use in integration tests with Ecto SQL Sandbox and a running Interruptus
  instance. Typical patterns:

      {:ok, %{id: id}} = Interruptus.start(MyWorkflow, params)
      assert {:ok, %{status: :completed}} = Interruptus.Test.await_status(id, :completed)

      Interruptus.Test.crash_runner(id)
      assert {:ok, %{status: :completed}} = Interruptus.Test.await_status(id, :completed)
  """

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
  Kills the runner process for a workflow to simulate a crash.

  After the lease expires, `Interruptus.Recovery` should reclaim and restart
  execution. Use with `await_status/3` to verify recovery behavior.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance

  ## Returns

    * `:ok` - runner was killed
    * `{:error, :not_running}` - no runner registered for this id
  """
  @spec crash_runner(Ecto.UUID.t()) :: :ok | {:error, :not_running}
  def crash_runner(workflow_id) do
    case Registry.lookup(Interruptus.Registry, workflow_id) do
      [{pid, _}] ->
        Process.exit(pid, :kill)
        :ok

      [] ->
        {:error, :not_running}
    end
  end

  @doc """
  Asserts that a checkpoint row exists at the given stage index.

  ## Arguments

    * `workflow_id` - UUID of the workflow instance
    * `stage_index` - zero-based checkpoint index

  ## Options

    * `:config` - Interruptus config (default `Interruptus.Config.fetch/0`)

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
end
