defmodule Interruptus.Claim do
  @moduledoc """
  Transactional lease acquisition, renewal, and release for workflow instances.

  Provides cluster-wide exclusivity: only the node holding a valid lease may
  update workflow state. Leases expire after `lease_duration` unless renewed
  by the runner heartbeat.
  """

  import Ecto.Query

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.WorkflowInstance

  @doc """
  Attempts to claim a workflow for execution by this node.

  Runs in a `FOR UPDATE` transaction. Sets `status` to `:running`, assigns
  `locked_by` to `config.node_id`, and extends `locked_until`.

  ## Arguments

    * `config` - Interruptus config with `node_id` and `lease_duration`
    * `workflow_id` - UUID of the workflow instance

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - claimed instance with updated lease fields
    * `{:error, :not_claimable}` - workflow is terminal or lease not expired
    * `{:error, :not_found}` - no row with that id
  """
  @spec acquire(Config.t(), Ecto.UUID.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :not_claimable | :not_found}
  def acquire(config, workflow_id) do
    now = DateTime.utc_now()
    locked_until = DateTime.add(now, config.lease_duration, :millisecond)

    Repo.transaction(config, fn ->
      instance =
        Repo.one!(
          config,
          from(w in WorkflowInstance,
            where: w.id == ^workflow_id,
            lock: "FOR UPDATE"
          )
        )

      if WorkflowInstance.claimable?(instance, config.node_id, now) do
        new_version = instance.lock_version + 1

        {1, _} =
          Interruptus.Repo.update_all(
            config,
            from(w in WorkflowInstance, where: w.id == ^workflow_id),
            set: [
              status: :running,
              locked_by: config.node_id,
              locked_until: locked_until,
              lock_version: new_version,
              updated_at: now
            ]
          )

        Map.merge(instance, %{
          status: :running,
          locked_by: config.node_id,
          locked_until: locked_until,
          lock_version: new_version
        })
      else
        config.repo.rollback(:not_claimable)
      end
    end)
    |> case do
      {:ok, instance} -> {:ok, instance}
      {:error, reason} -> {:error, reason}
    end
  rescue
    Ecto.NoResultsError -> {:error, :not_found}
  end

  @doc """
  Renews the lease for a workflow claimed by this node.

  Extends `locked_until` by `lease_duration` from now. Only the current
  lease holder (`locked_by == config.node_id`) may renew.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - claimed instance with current `id`, `locked_by`, and `lock_version`

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - instance with extended lease
    * `{:error, :not_holder}` - `locked_by` does not match this node
    * `{:error, :stale_lock}` - `lock_version` changed since claim
  """
  @spec renew(Config.t(), WorkflowInstance.t()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock | :not_holder}
  def renew(config, %WorkflowInstance{id: id, locked_by: locked_by, lock_version: version}) do
    if locked_by != config.node_id do
      {:error, :not_holder}
    else
      now = DateTime.utc_now()
      locked_until = DateTime.add(now, config.lease_duration, :millisecond)

      Interruptus.Store.update_with_lock(
        config,
        %WorkflowInstance{id: id, lock_version: version},
        %{locked_until: locked_until}
      )
    end
  end

  @doc """
  Releases the lease without changing workflow status.

  Clears `locked_by` and `locked_until`. Called when a runner terminates.

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
end
