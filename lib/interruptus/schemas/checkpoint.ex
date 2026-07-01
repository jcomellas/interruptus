defmodule Interruptus.Schemas.Checkpoint do
  @moduledoc """
  Ecto schema for `interruptus_checkpoints` table.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

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

  @doc false
  def changeset(checkpoint, attrs) do
    checkpoint
    |> cast(attrs, [:workflow_id, :stage_index, :params, :data, :inserted_at])
    |> validate_required([:workflow_id, :stage_index, :params, :data])
    |> put_inserted_at()
  end

  defp put_inserted_at(changeset) do
    case get_field(changeset, :inserted_at) do
      nil -> put_change(changeset, :inserted_at, DateTime.utc_now())
      _ -> changeset
    end
  end
end
