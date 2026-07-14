defmodule Interruptus.Schemas.Effect do
  @moduledoc """
  Ecto schema for the `interruptus_effects` table.

  Durable markers for stage side effects, keyed uniquely by
  `(workflow_id, effect_key)`. Used by `Interruptus.Effect` to skip already
  applied work on at-least-once re-execution.

  ## Fields

    * `:workflow_id` - parent workflow instance UUID
    * `:effect_key` - host-chosen idempotency key for the effect
    * `:metadata` - optional JSON map of effect details
    * `:inserted_at` - marker timestamp
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_id: Ecto.UUID.t(),
          effect_key: String.t(),
          metadata: map(),
          inserted_at: DateTime.t()
        }

  schema "interruptus_effects" do
    field :effect_key, :string
    field :metadata, :map, default: %{}

    belongs_to :workflow, Interruptus.Schemas.WorkflowInstance,
      foreign_key: :workflow_id,
      define_field: false

    field :workflow_id, :binary_id

    field :inserted_at, :utc_datetime_usec
  end

  # Builds a changeset for effect insert. Used by Interruptus.Effect.
  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(effect, attrs) do
    effect
    |> cast(attrs, [:workflow_id, :effect_key, :metadata, :inserted_at])
    |> validate_required([:workflow_id, :effect_key])
    |> unique_constraint([:workflow_id, :effect_key],
      name: :interruptus_effects_workflow_key_idx
    )
    |> put_inserted_at()
  end

  @spec put_inserted_at(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  defp put_inserted_at(changeset) do
    case get_field(changeset, :inserted_at) do
      nil -> put_change(changeset, :inserted_at, DateTime.utc_now())
      _ -> changeset
    end
  end
end
