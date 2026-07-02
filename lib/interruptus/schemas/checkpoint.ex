defmodule Interruptus.Schemas.Checkpoint do
  @moduledoc """
  Ecto schema for the `interruptus_checkpoints` table.

  Historical audit trail of workflow state at each checkpoint boundary.
  A row is written when a checkpoint segment completes and on initial insert.

  ## Fields

    * `:workflow_id` - parent workflow instance UUID
    * `:stage_index` - `current_stage_index` at snapshot time
    * `:params` - serialized workflow params (JSONB)
    * `:data` - serialized workflow data (JSONB)
    * `:inserted_at` - snapshot timestamp
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_id: Ecto.UUID.t(),
          stage_index: integer(),
          params: map(),
          data: map(),
          inserted_at: DateTime.t()
        }

  schema "interruptus_checkpoints" do
    field :stage_index, :integer
    field :params, :map, default: %{}
    field :data, :map, default: %{}

    belongs_to :workflow, Interruptus.Schemas.WorkflowInstance,
      foreign_key: :workflow_id,
      define_field: false

    field :workflow_id, :binary_id

    field :inserted_at, :utc_datetime_usec
  end

  # Builds a changeset for checkpoint insert. Used by Interruptus.Store.
  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(checkpoint, attrs) do
    checkpoint
    |> cast(attrs, [:workflow_id, :stage_index, :params, :data, :inserted_at])
    |> validate_required([:workflow_id, :stage_index, :params, :data])
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
