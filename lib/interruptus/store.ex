defmodule Interruptus.Store do
  @moduledoc """
  Persistence layer for workflow instances, checkpoints, and stage attempts.

  All functions take an `Interruptus.Config` as the first argument and operate
  through `Interruptus.Repo` against the host application's database.
  """

  import Ecto.Query

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.Checkpoint
  alias Interruptus.Schemas.StageAttempt
  alias Interruptus.Schemas.WorkflowInstance

  @doc """
  Inserts a new workflow instance with an initial checkpoint.

  Runs in a transaction: inserts the instance row, then writes a checkpoint
  snapshot at `current_stage_index`.

  ## Arguments

    * `config` - Interruptus config
    * `attrs` - map of workflow instance attributes (see `WorkflowInstance` schema)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - inserted instance
    * `{:error, %Ecto.Changeset{}}` - validation or constraint failure
  """
  @spec insert_workflow(Config.t(), map()) ::
          {:ok, WorkflowInstance.t()} | {:error, Ecto.Changeset.t()}
  def insert_workflow(config, attrs) do
    Repo.transaction(config, fn ->
      with {:ok, instance} <-
             %WorkflowInstance{}
             |> WorkflowInstance.changeset(attrs)
             |> then(&Repo.insert(config, &1)),
           {:ok, _checkpoint} <- insert_checkpoint(config, instance) do
        instance
      else
        {:error, changeset} -> config.repo.rollback(changeset)
      end
    end)
  end

  @doc """
  Fetches a workflow instance by id.

  ## Arguments

    * `config` - Interruptus config
    * `id` - workflow UUID

  ## Returns

    * `%WorkflowInstance{}` when found
    * `nil` when no row exists
  """
  @spec get(Config.t(), Ecto.UUID.t()) :: WorkflowInstance.t() | nil
  def get(config, id) do
    Repo.one(config, from(w in WorkflowInstance, where: w.id == ^id))
  end

  @doc """
  Updates workflow fields with optimistic locking on `lock_version`.

  The update succeeds only when the row's `lock_version` matches the instance
  passed in. On success, `lock_version` is incremented automatically.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - instance with current `id` and `lock_version`
    * `attrs` - map of fields to update

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - freshly loaded row after update
    * `{:error, :stale_lock}` - another process updated the row first
  """
  @spec update_with_lock(Config.t(), WorkflowInstance.t() | WorkflowInstance.lock_ref(), map()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock | Ecto.Changeset.t()}
  def update_with_lock(config, %WorkflowInstance{id: id, lock_version: version}, attrs) do
    now = DateTime.utc_now()
    set_fields = build_set_fields(attrs, now)

    {count, _} =
      Repo.update_all(
        config,
        from(w in WorkflowInstance,
          where: w.id == ^id and w.lock_version == ^version
        ),
        set: set_fields
      )

    case count do
      1 -> {:ok, get!(config, id)}
      0 -> {:error, :stale_lock}
    end
  end

  @doc """
  Writes a checkpoint snapshot for the workflow.

  Persists `params`, `data`, and `stage_index` from the current instance state
  into `interruptus_checkpoints`.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - workflow instance to snapshot

  ## Returns

    * `{:ok, %Checkpoint{}}` - inserted checkpoint row
    * `{:error, %Ecto.Changeset{}}` - validation failure
  """
  @spec write_checkpoint(Config.t(), WorkflowInstance.t()) ::
          {:ok, Checkpoint.t()} | {:error, Ecto.Changeset.t()}
  def write_checkpoint(config, %WorkflowInstance{} = instance) do
    insert_checkpoint(config, instance)
  end

  @doc """
  Logs a stage attempt outcome.

  ## Arguments

    * `config` - Interruptus config
    * `attrs` - map with `:workflow_id`, `:stage_name`, `:attempt_number`, `:outcome`,
      and optional `:error`

  ## Returns

    * `{:ok, %StageAttempt{}}` - inserted attempt row
    * `{:error, %Ecto.Changeset{}}` - validation failure
  """
  @spec log_attempt(Config.t(), StageAttempt.attempt_attrs()) ::
          {:ok, StageAttempt.t()} | {:error, Ecto.Changeset.t()}
  def log_attempt(config, attrs) do
    %StageAttempt{}
    |> StageAttempt.changeset(attrs)
    |> then(&Repo.insert(config, &1))
  end

  @doc """
  Lists workflows that are reclaimable after lease expiry.

  Returns non-terminal workflows (`:pending`, `:suspended`, `:running`) whose
  `locked_until` is `nil` or in the past. Used by `Interruptus.Recovery`.

  ## Arguments

    * `config` - Interruptus config
    * `now` - reference time for lease comparison (default `DateTime.utc_now/0`)

  ## Returns

    * List of `%WorkflowInstance{}`, ordered by `inserted_at`, limited to 100 rows
  """
  @spec list_reclaimable(Config.t(), DateTime.t()) :: [WorkflowInstance.t()]
  def list_reclaimable(config, now \\ DateTime.utc_now()) do
    Repo.all(
      config,
      from(w in WorkflowInstance,
        where:
          w.status in [:pending, :suspended, :running] and
            (is_nil(w.locked_until) or w.locked_until < ^now),
        order_by: [asc: w.inserted_at],
        limit: 100
      )
    )
  end

  @spec build_set_fields(map(), DateTime.t()) :: [{atom(), term()}]
  defp build_set_fields(attrs, now) do
    attrs
    |> Map.put(:updated_at, now)
    |> Enum.map(fn {k, v} -> {k, v} end)
  end

  @spec insert_checkpoint(Config.t(), WorkflowInstance.t()) ::
          {:ok, Checkpoint.t()} | {:error, Ecto.Changeset.t()}
  defp insert_checkpoint(config, %WorkflowInstance{} = instance) do
    %Checkpoint{}
    |> Checkpoint.changeset(%{
      workflow_id: instance.id,
      stage_index: instance.current_stage_index,
      params: instance.params,
      data: instance.data
    })
    |> then(&Repo.insert(config, &1))
  end

  @spec get!(Config.t(), Ecto.UUID.t()) :: WorkflowInstance.t()
  defp get!(config, id) do
    Repo.one!(config, from(w in WorkflowInstance, where: w.id == ^id))
  end
end
