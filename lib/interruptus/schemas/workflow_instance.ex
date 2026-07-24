defmodule Interruptus.Schemas.WorkflowInstance do
  @moduledoc """
  Ecto schema for the `interruptus_workflows` table.

  Represents a single durable workflow execution. Params and data must be
  JSON-serializable maps (string keys in the database).

  ## Status lifecycle

    * `:pending` — inserted (or resumed), awaiting claim
    * `:running` — runner holds a lease and is executing
    * `:suspended` — voluntarily paused; only `Interruptus.resume/2` restarts it
    * `:completed` — all segments finished successfully (terminal)
    * `:failed` — failure with no compensations to run, or compensation
      exhaustion; `Interruptus.resume/2` retries compensation
    * `:compensating` — rollback in progress; reclaimable after lease expiry
    * `:compensated` — rollback completed (terminal)
    * `:cancelled` — cancelled via `Interruptus.cancel/2` (terminal)

  Terminal statuses (`:completed`, `:compensated`, `:cancelled`) are never restarted.

  ## Lease and fencing fields

    * `locked_by` — node id of the claiming runner
    * `locked_until` — lease expiry; reclaimable when in the past
    * `lock_version` — fencing token. Incremented by **every** state-changing
      write (claim, checkpoint, attempt accounting, status transitions, cancel,
      resume). Lease renewal (`Interruptus.Claim.renew/2`) extends `locked_until`
      without bumping the version so external API writes do not race heartbeats.

  ## Progress fields

    * `current_stage_index` — next flattened segment to execute
    * `pipeline_fingerprint` — structural hash of the compiled pipeline layout
    * `attempt_count` — persisted **before** each execution attempt of the
      current segment span; reset to `0` on every successful checkpoint
    * `compensation_index` — number of compensation functions already applied
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @type status ::
          :pending
          | :running
          | :suspended
          | :completed
          | :failed
          | :compensating
          | :compensated
          | :cancelled

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_type: String.t(),
          status: status(),
          params: map(),
          data: map(),
          current_stage_index: integer(),
          pipeline_version: integer(),
          pipeline_fingerprint: String.t(),
          idempotency_key: String.t() | nil,
          locked_by: String.t() | nil,
          locked_until: DateTime.t() | nil,
          lock_version: integer(),
          attempt_count: integer(),
          compensation_index: integer(),
          suspend_reason: String.t() | nil,
          suspend_metadata: map() | nil,
          errors: map(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  @type lock_ref :: %__MODULE__{
          id: Ecto.UUID.t(),
          lock_version: non_neg_integer()
        }

  @statuses ~w(
    pending running suspended completed failed
    compensating compensated cancelled
  )a

  @claimable_statuses ~w(pending running compensating)a

  schema "interruptus_workflows" do
    field :workflow_type, :string
    field :status, Ecto.Enum, values: @statuses, default: :pending
    field :params, :map, default: %{}
    field :data, :map, default: %{}
    field :current_stage_index, :integer, default: 0
    field :pipeline_version, :integer, default: 1
    field :pipeline_fingerprint, :string, default: ""
    field :idempotency_key, :string
    field :locked_by, :string
    field :locked_until, :utc_datetime_usec
    field :lock_version, :integer, default: 0
    field :attempt_count, :integer, default: 0
    field :compensation_index, :integer, default: 0
    field :suspend_reason, :string
    field :suspend_metadata, :map
    field :errors, :map, default: %{}

    has_many :checkpoints, Interruptus.Schemas.Checkpoint, foreign_key: :workflow_id
    has_many :stage_attempts, Interruptus.Schemas.StageAttempt, foreign_key: :workflow_id
    has_many :effects, Interruptus.Schemas.Effect, foreign_key: :workflow_id

    timestamps(type: :utc_datetime_usec)
  end

  # Builds a changeset for insert and update. Used internally by Interruptus.Store.
  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(instance, attrs) do
    instance
    |> cast(attrs, [
      :workflow_type,
      :status,
      :params,
      :data,
      :current_stage_index,
      :pipeline_version,
      :pipeline_fingerprint,
      :idempotency_key,
      :locked_by,
      :locked_until,
      :lock_version,
      :attempt_count,
      :compensation_index,
      :suspend_reason,
      :suspend_metadata,
      :errors
    ])
    |> validate_required([:workflow_type, :status])
    |> validate_inclusion(:status, @statuses)
    |> unique_constraint(:idempotency_key, name: :interruptus_workflows_idempotency_idx)
  end

  @doc """
  Returns whether the workflow is in a terminal state.

  Terminal workflows cannot be resumed, cancelled again, or reclaimed.

  ## Arguments

    * `instance` - workflow instance struct

  ## Returns

    * `true` when status is `:completed`, `:compensated`, or `:cancelled`
    * `false` otherwise

  ## Examples

      iex> instance = %Interruptus.Schemas.WorkflowInstance{status: :completed}
      iex> Interruptus.Schemas.WorkflowInstance.terminal?(instance)
      true
      iex> instance = %Interruptus.Schemas.WorkflowInstance{status: :running}
      iex> Interruptus.Schemas.WorkflowInstance.terminal?(instance)
      false
  """
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{status: status}) do
    status in [:completed, :compensated, :cancelled]
  end

  @doc """
  Returns whether the workflow can be claimed for execution.

  Claimable when status is `:pending`, `:running`, or `:compensating` and the
  lease has expired (`locked_until` is `nil` or before `now`).

  `:suspended` workflows are **not** claimable: only an explicit
  `Interruptus.resume/2` (which transitions them to `:pending`) makes them
  runnable again. `:failed` workflows are resumed into `:compensating` the same
  way.

  ## Arguments

    * `instance` - workflow instance struct
    * `_node_id` - claiming node id (reserved for future sticky assignment)
    * `now` - reference time for lease comparison

  ## Returns

    * `true` when the instance may be claimed
    * `false` otherwise

  ## Examples

      iex> now = ~U[2025-01-01 12:00:00Z]
      iex> instance = %Interruptus.Schemas.WorkflowInstance{
      ...>   status: :pending,
      ...>   locked_until: nil
      ...> }
      iex> Interruptus.Schemas.WorkflowInstance.claimable?(instance, "node@host", now)
      true
      iex> instance = %Interruptus.Schemas.WorkflowInstance{
      ...>   status: :suspended,
      ...>   locked_until: nil
      ...> }
      iex> Interruptus.Schemas.WorkflowInstance.claimable?(instance, "node@host", now)
      false
      iex> instance = %Interruptus.Schemas.WorkflowInstance{
      ...>   status: :running,
      ...>   locked_until: ~U[2025-01-01 13:00:00Z]
      ...> }
      iex> Interruptus.Schemas.WorkflowInstance.claimable?(instance, "node@host", now)
      false
  """
  @spec claimable?(t(), String.t(), DateTime.t()) :: boolean()
  def claimable?(%__MODULE__{status: status, locked_until: locked_until}, _node_id, now) do
    status in @claimable_statuses and lease_expired?(locked_until, now)
  end

  @doc false
  @spec claimable_statuses() :: [status()]
  def claimable_statuses, do: @claimable_statuses

  @spec lease_expired?(DateTime.t() | nil, DateTime.t()) :: boolean()
  defp lease_expired?(nil, _now), do: true

  defp lease_expired?(locked_until, now) do
    DateTime.compare(locked_until, now) == :lt
  end
end
