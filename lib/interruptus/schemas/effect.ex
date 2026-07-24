defmodule Interruptus.Schemas.Effect do
  @moduledoc """
  Ecto schema for the `interruptus_effects` table.

  Durable markers for stage side effects, keyed uniquely by
  `(workflow_id, effect_key)`. Used by `Interruptus.Effect` to claim work
  (`:pending`) and mark successful completion (`:applied`) so at-least-once
  re-execution can skip finished effects and reclaim stale claims.

  ## Fields

    * `:workflow_id` - parent workflow instance UUID
    * `:effect_key` - host-chosen idempotency key for the effect
    * `:status` - `:pending` while applying, `:applied` when done
    * `:metadata` - optional JSON map of effect details
    * `:inserted_at` / `:updated_at` - timestamps (stale pending reclaim uses `updated_at`)
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @statuses ~w(pending applied)a

  @type status :: :pending | :applied

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_id: Ecto.UUID.t(),
          effect_key: String.t(),
          status: status(),
          metadata: map(),
          inserted_at: DateTime.t(),
          updated_at: DateTime.t()
        }

  schema "interruptus_effects" do
    field :effect_key, :string
    field :status, Ecto.Enum, values: @statuses, default: :pending
    field :metadata, :map, default: %{}

    belongs_to :workflow, Interruptus.Schemas.WorkflowInstance,
      foreign_key: :workflow_id,
      define_field: false

    field :workflow_id, :binary_id

    field :inserted_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end

  # Builds a changeset for effect insert/update. Used by Interruptus.Effect.
  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(effect, attrs) do
    now = DateTime.utc_now()

    effect
    |> cast(attrs, [:workflow_id, :effect_key, :status, :metadata, :inserted_at, :updated_at])
    |> validate_required([:workflow_id, :effect_key, :status])
    |> validate_inclusion(:status, @statuses)
    |> unique_constraint([:workflow_id, :effect_key],
      name: :interruptus_effects_workflow_key_idx
    )
    |> maybe_put_timestamps(now)
  end

  @spec maybe_put_timestamps(Ecto.Changeset.t(), DateTime.t()) :: Ecto.Changeset.t()
  defp maybe_put_timestamps(changeset, now) do
    changeset
    |> then(fn cs ->
      case get_field(cs, :inserted_at) do
        nil -> put_change(cs, :inserted_at, now)
        _ -> cs
      end
    end)
    |> then(fn cs ->
      case get_change(cs, :updated_at) do
        nil -> put_change(cs, :updated_at, now)
        _ -> cs
      end
    end)
  end
end
