defmodule Interruptus.Test do
  @moduledoc """
  Test helpers for simulating interrupts and asserting workflow state.
  """

  alias Interruptus.Config
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  @doc """
  Waits until the workflow reaches the given status or times out.
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
  Asserts the workflow has a checkpoint at the given stage index.
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
