defmodule Interruptus.Claim do
  @moduledoc """
  Transactional lease acquisition, renewal, and release for workflow instances.

  Provides cluster-wide exclusivity: only the node holding a valid lease may
  update workflow state. Leases expire after `lease_duration` unless renewed
  by the runner heartbeat.

  ## Fencing

  `acquire/2` increments `lock_version`, fencing out any process holding a
  stale snapshot. `renew/2` extends `locked_until` **without** bumping the
  version: renewal is lease maintenance, not a state change, and bumping on
  every heartbeat would make external writes (`Interruptus.cancel/2`) race
  the heartbeat spuriously. `release/2` clears the lease through
  `Interruptus.Store.update_with_lock/3` (which bumps the version).
  """

  import Ecto.Query

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.WorkflowInstance

  @doc """
  Attempts to claim a workflow for execution by this node.

  Runs in a `FOR UPDATE SKIP LOCKED` transaction so competing claimers do not
  serialize on the same row. Sets `status` to `:running` (or preserves
  `:compensating` for workflows reclaimed mid-rollback), assigns `locked_by`
  to `config.node_id`, extends `locked_until`, and increments `lock_version`.

  ## Arguments

    * `config` - Interruptus config with `node_id` and `lease_duration`
    * `workflow_id` - UUID of the workflow instance

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - claimed instance with updated lease fields
    * `{:error, :not_claimable}` - workflow is terminal, suspended, lease not
      expired, or currently row-locked by a competing claimer
    * `{:error, :not_found}` - no row with that id
    * `{:error, :in_transaction}` - call site is already inside a DB transaction
  """
  @spec acquire(Config.t(), Ecto.UUID.t()) ::
          {:ok, WorkflowInstance.t()}
          | {:error, :not_claimable | :not_found | :in_transaction}
  def acquire(config, workflow_id) do
    if Repo.in_transaction?(config) do
      {:error, :in_transaction}
    else
      do_acquire(config, workflow_id)
    end
  end

  @spec do_acquire(Config.t(), Ecto.UUID.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :not_claimable | :not_found}
  defp do_acquire(config, workflow_id) do
    now = DateTime.utc_now()
    locked_until = DateTime.add(now, config.lease_duration, :millisecond)

    Repo.transaction(config, fn ->
      case Repo.one(
             config,
             from(w in WorkflowInstance,
               where: w.id == ^workflow_id,
               lock: "FOR UPDATE SKIP LOCKED"
             )
           ) do
        nil ->
          config.repo.rollback(:row_unavailable)

        instance ->
          if WorkflowInstance.claimable?(instance, config.node_id, now) do
            new_version = instance.lock_version + 1
            new_status = claim_status(instance.status)

            {1, _} =
              Interruptus.Repo.update_all(
                config,
                from(w in WorkflowInstance, where: w.id == ^workflow_id),
                set: [
                  status: new_status,
                  locked_by: config.node_id,
                  locked_until: locked_until,
                  lock_version: new_version,
                  updated_at: now
                ]
              )

            Map.merge(instance, %{
              status: new_status,
              locked_by: config.node_id,
              locked_until: locked_until,
              lock_version: new_version
            })
          else
            config.repo.rollback(:not_claimable)
          end
      end
    end)
    |> case do
      {:ok, instance} ->
        {:ok, instance}

      {:error, :row_unavailable} ->
        # Either the row does not exist or it is row-locked by a competing
        # claim transaction (SKIP LOCKED). Distinguish with a plain read.
        if exists?(config, workflow_id) do
          {:error, :not_claimable}
        else
          {:error, :not_found}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Renews the lease for a workflow claimed by this node.

  Extends `locked_until` by `lease_duration` from now. Only the current
  lease holder (`locked_by == config.node_id`) with a matching `lock_version`
  may renew. The version is **not** incremented (see moduledoc).

  ## Arguments

    * `config` - Interruptus config
    * `instance` - claimed instance with current `id`, `locked_by`, and `lock_version`

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - instance with extended lease
    * `{:error, :not_holder}` - `locked_by` does not match this node
    * `{:error, :stale_lock}` - `lock_version` or `locked_by` changed since claim
  """
  @spec renew(Config.t(), WorkflowInstance.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock | :not_holder}
  def renew(config, %WorkflowInstance{id: id, locked_by: locked_by, lock_version: version} = instance) do
    if locked_by != config.node_id do
      {:error, :not_holder}
    else
      now = DateTime.utc_now()
      locked_until = DateTime.add(now, config.lease_duration, :millisecond)

      {count, _} =
        Repo.update_all(
          config,
          from(w in WorkflowInstance,
            where: w.id == ^id and w.lock_version == ^version and w.locked_by == ^config.node_id
          ),
          set: [locked_until: locked_until, updated_at: now]
        )

      case count do
        1 -> {:ok, %{instance | locked_until: locked_until}}
        0 -> {:error, :stale_lock}
      end
    end
  end

  @doc """
  Releases the lease without changing workflow status.

  Clears `locked_by` and `locked_until` and bumps `lock_version`. Called when
  a runner terminates while still holding the lease.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - instance with current `id` and `lock_version`

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - instance with lease cleared
    * `{:error, :stale_lock}` - `lock_version` changed since last read
  """
  @spec release(Config.t(), WorkflowInstance.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock}
  def release(config, %WorkflowInstance{} = instance) do
    Interruptus.Store.update_with_lock(config, instance, %{
      locked_by: nil,
      locked_until: nil
    })
  end

  @spec claim_status(WorkflowInstance.status()) :: WorkflowInstance.status()
  defp claim_status(:compensating), do: :compensating
  defp claim_status(_status), do: :running

  @spec exists?(Config.t(), Ecto.UUID.t()) :: boolean()
  defp exists?(config, workflow_id) do
    Repo.one(
      config,
      from(w in WorkflowInstance, where: w.id == ^workflow_id, select: 1, limit: 1)
    ) == 1
  end
end
