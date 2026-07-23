defmodule Interruptus.Store do
  @moduledoc """
  Persistence layer for workflow instances, checkpoints, and stage attempts.

  All functions take an `Interruptus.Config` as the first argument and operate
  through `Interruptus.Repo` against the host application's database.

  ## Fencing

  Every state-changing write goes through optimistic locking on `lock_version`
  and **increments the version**, so any process holding a stale snapshot of
  the row is fenced out on its next write:

    * `update_with_lock/3` — version check + bump. Used by API-side writes
      (`Interruptus.cancel/2`, `Interruptus.resume/2`) and lease release.
    * `update_as_holder/4` — version check + bump, additionally guarded by
      `locked_by = node_id AND locked_until > now()`. Used for all
      runner-originated writes so an expired-lease runner cannot write even
      before another node re-claims the row.
    * `checkpoint_progress/4` — `update_as_holder/4` plus checkpoint audit
      insert in a single transaction.
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
  Fetches a workflow by `workflow_type` and `idempotency_key`.

  Used to return the existing instance when `Interruptus.start/3` hits the
  idempotency unique index.

  ## Arguments

    * `config` - Interruptus config
    * `workflow_type` - workflow type string
    * `idempotency_key` - idempotency key string

  ## Returns

    * `%WorkflowInstance{}` when found
    * `nil` when no row exists
  """
  @spec get_by_idempotency_key(Config.t(), String.t(), String.t()) ::
          WorkflowInstance.t() | nil
  def get_by_idempotency_key(config, workflow_type, idempotency_key) do
    Repo.one(
      config,
      from(w in WorkflowInstance,
        where: w.workflow_type == ^workflow_type and w.idempotency_key == ^idempotency_key
      )
    )
  end

  @doc """
  Updates workflow fields with optimistic locking on `lock_version`.

  The update succeeds only when the row's `lock_version` matches the instance
  passed in. On success, `lock_version` is incremented (fencing token) and the
  freshly loaded row is returned.

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
    set_fields = build_set_fields(attrs, version, now)

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
  Updates workflow fields as the current lease holder.

  Like `update_with_lock/3` but additionally requires the row to be held by
  `node_id` with an unexpired lease. All runner-originated writes must use this
  function so a runner whose lease expired (or was fenced by `cancel`/`resume`)
  cannot mutate the row.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - instance with current `id` and `lock_version`
    * `node_id` - the writing node; must match `locked_by`
    * `attrs` - map of fields to update

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - freshly loaded row after update
    * `{:error, :stale_lock}` - version mismatch, foreign holder, or expired lease
  """
  @spec update_as_holder(
          Config.t(),
          WorkflowInstance.t() | WorkflowInstance.lock_ref(),
          String.t(),
          map()
        ) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock}
  def update_as_holder(config, %WorkflowInstance{id: id, lock_version: version}, node_id, attrs) do
    now = DateTime.utc_now()
    set_fields = build_set_fields(attrs, version, now)

    {count, _} =
      Repo.update_all(
        config,
        from(w in WorkflowInstance,
          where:
            w.id == ^id and w.lock_version == ^version and w.locked_by == ^node_id and
              w.locked_until > ^now
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
  Atomically advances workflow progress and writes a checkpoint audit row.

  Runs the holder-guarded optimistic lock update and checkpoint insert in a
  single transaction so the instance row and audit trail cannot diverge
  mid-crash. Only the current lease holder may checkpoint.

  ## Arguments

    * `config` - Interruptus config
    * `instance` - instance with current `id` and `lock_version`
    * `node_id` - the writing node; must match `locked_by`
    * `attrs` - fields to update on the workflow row (typically `params`, `data`,
      `current_stage_index`, `attempt_count`, `errors`)

  ## Returns

    * `{:ok, %WorkflowInstance{}}` - freshly loaded row after update
    * `{:error, :stale_lock}` - version mismatch, foreign holder, or expired lease
    * `{:error, %Ecto.Changeset{}}` - checkpoint insert validation failure
  """
  @spec checkpoint_progress(Config.t(), WorkflowInstance.t(), String.t(), map()) ::
          {:ok, WorkflowInstance.t()} | {:error, :stale_lock | Ecto.Changeset.t()}
  def checkpoint_progress(config, %WorkflowInstance{id: id, lock_version: version}, node_id, attrs) do
    now = DateTime.utc_now()
    set_fields = build_set_fields(attrs, version, now)

    Repo.transaction(config, fn ->
      {count, _} =
        Repo.update_all(
          config,
          from(w in WorkflowInstance,
            where:
              w.id == ^id and w.lock_version == ^version and w.locked_by == ^node_id and
                w.locked_until > ^now
          ),
          set: set_fields
        )

      case count do
        0 ->
          config.repo.rollback(:stale_lock)

        1 ->
          updated = get!(config, id)

          case insert_checkpoint(config, updated) do
            {:ok, _} ->
              updated

            {:error, changeset} ->
              config.repo.rollback(changeset)
          end
      end
    end)
    |> case do
      {:ok, updated} -> {:ok, updated}
      {:error, reason} -> {:error, reason}
    end
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

  Returns `:pending`, `:running`, and `:compensating` workflows whose
  `locked_until` is `nil` or in the past. `:suspended` workflows are **not**
  reclaimable — they require an explicit `Interruptus.resume/2`. Used by
  `Interruptus.Recovery`.

  ## Arguments

    * `config` - Interruptus config
    * `now` - reference time for lease comparison (default `DateTime.utc_now/0`)
    * `opts` - pagination options

  ## Options

    * `:limit` - page size (default `100`)
    * `:after` - `{inserted_at, id}` cursor for keyset pagination (exclusive)

  ## Returns

    * List of `%WorkflowInstance{}`, ordered by `inserted_at`, `id`
  """
  @spec list_reclaimable(Config.t(), DateTime.t(), keyword()) :: [WorkflowInstance.t()]
  def list_reclaimable(config, now \\ DateTime.utc_now(), opts \\ []) do
    statuses = WorkflowInstance.claimable_statuses()
    limit = Keyword.get(opts, :limit, 100)
    after_cursor = Keyword.get(opts, :after)

    query =
      from(w in WorkflowInstance,
        where:
          w.status in ^statuses and
            (is_nil(w.locked_until) or w.locked_until < ^now),
        order_by: [asc: w.inserted_at, asc: w.id],
        limit: ^limit
      )

    query =
      case after_cursor do
        {inserted_at, id} ->
          from(w in query,
            where:
              w.inserted_at > ^inserted_at or
                (w.inserted_at == ^inserted_at and w.id > ^id)
          )

        nil ->
          query
      end

    Repo.all(config, query)
  end

  @spec build_set_fields(map(), non_neg_integer(), DateTime.t()) :: [{atom(), term()}]
  defp build_set_fields(attrs, current_version, now) do
    attrs
    |> Map.drop([:lock_version])
    |> Map.put(:updated_at, now)
    |> Map.put(:lock_version, current_version + 1)
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
