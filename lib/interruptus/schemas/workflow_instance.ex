defmodule Interruptus.Schemas.WorkflowInstance do
  @moduledoc """
  Ecto schema for `interruptus_workflows` table.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_type: String.t(),
          status: atom(),
          params: map(),
          data: map(),
          current_stage_index: integer(),
          pipeline_version: integer(),
          idempotency_key: String.t() | nil,
          locked_by: String.t() | nil,
          locked_until: DateTime.t() | nil,
          lock_version: integer(),
          attempt_count: integer(),
          suspend_reason: String.t() | nil,
          suspend_metadata: map() | nil,
          errors: map(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @statuses ~w(
    pending running suspended completed failed
    compensating compensated cancelled
  )a

  schema "interruptus_workflows" do
    field :workflow_type, :string
    field :status, Ecto.Enum, values: @statuses, default: :pending
    field :params, :map, default: %{}
    field :data, :map, default: %{}
    field :current_stage_index, :integer, default: 0
    field :pipeline_version, :integer, default: 1
    field :idempotency_key, :string
    field :locked_by, :string
    field :locked_until, :utc_datetime_usec
    field :lock_version, :integer, default: 0
    field :attempt_count, :integer, default: 0
    field :suspend_reason, :string
    field :suspend_metadata, :map
    field :errors, :map, default: %{}

    has_many :checkpoints, Interruptus.Schemas.Checkpoint, foreign_key: :workflow_id
    has_many :stage_attempts, Interruptus.Schemas.StageAttempt, foreign_key: :workflow_id

    timestamps(type: :utc_datetime_usec)
  end

  @doc false
  def changeset(instance, attrs) do
    instance
    |> cast(attrs, [
      :workflow_type,
      :status,
      :params,
      :data,
      :current_stage_index,
      :pipeline_version,
      :idempotency_key,
      :locked_by,
      :locked_until,
      :lock_version,
      :attempt_count,
      :suspend_reason,
      :suspend_metadata,
      :errors
    ])
    |> validate_required([:workflow_type, :status])
    |> validate_inclusion(:status, @statuses)
  end

  @doc """
  Returns whether the workflow is in a terminal state.
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}) do
    status in [:completed, :compensated, :cancelled]
  end

  @doc """
  Returns whether the workflow can be claimed for execution.
  """
  @spec claimable?(t(), String.t(), DateTime.t()) :: boolean()
  def claimable?(%__MODULE__{status: status, locked_until: locked_until}, _node_id, now) do
    status in [:pending, :suspended, :running] and lease_expired?(locked_until, now)
  end

  defp lease_expired?(nil, _now), do: true

  defp lease_expired?(locked_until, now) do
    DateTime.compare(locked_until, now) == :lt
  end
end
