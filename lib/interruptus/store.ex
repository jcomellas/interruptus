defmodule Interruptus.Store do
  @moduledoc """
  Persistence layer for workflow instances, checkpoints, and stage attempts.
  """

  import Ecto.Query

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.Checkpoint
  alias Interruptus.Schemas.StageAttempt
  alias Interruptus.Schemas.WorkflowInstance

  @doc """
  Inserts a new workflow instance with an initial checkpoint.
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
  Fetches a workflow by id.
  """
  @spec get(Config.t(), Ecto.UUID.t()) :: WorkflowInstance.t() | nil
  def get(config, id) do
    Repo.one(config, from(w in WorkflowInstance, where: w.id == ^id))
  end

  @doc """
  Updates workflow fields with optimistic lock on lock_version.
  """
  @spec update_with_lock(Config.t(), WorkflowInstance.t(), map()) ::
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
  """
  @spec write_checkpoint(Config.t(), WorkflowInstance.t()) ::
          {:ok, Checkpoint.t()} | {:error, Ecto.Changeset.t()}
  def write_checkpoint(config, %WorkflowInstance{} = instance) do
    insert_checkpoint(config, instance)
  end

  @doc """
  Logs a stage attempt.
  """
  @spec log_attempt(Config.t(), map()) ::
          {:ok, StageAttempt.t()} | {:error, Ecto.Changeset.t()}
  def log_attempt(config, attrs) do
    %StageAttempt{}
    |> StageAttempt.changeset(attrs)
    |> then(&Repo.insert(config, &1))
  end

  @doc """
  Lists workflows that are reclaimable (non-terminal, expired lease).
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

  defp build_set_fields(attrs, now) do
    attrs
    |> Map.put(:updated_at, now)
    |> Enum.map(fn {k, v} -> {k, v} end)
  end

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

  defp get!(config, id) do
    Repo.one!(config, from(w in WorkflowInstance, where: w.id == ^id))
  end
end
