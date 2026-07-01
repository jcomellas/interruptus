defmodule Interruptus.Schemas.StageAttempt do
  @moduledoc """
  Ecto schema for `interruptus_stage_attempts` table.
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @outcomes ~w(success failure suspended halted timeout verify_done verify_not_done verify_failed)a

  schema "interruptus_stage_attempts" do
    field :stage_name, :string
    field :attempt_number, :integer, default: 1
    field :outcome, Ecto.Enum, values: @outcomes
    field :error, :map

    belongs_to :workflow, Interruptus.Schemas.WorkflowInstance,
      foreign_key: :workflow_id,
      define_field: false

    field :workflow_id, :binary_id

    field :inserted_at, :utc_datetime_usec
  end

  @doc false
  def changeset(attempt, attrs) do
    attempt
    |> cast(attrs, [:workflow_id, :stage_name, :attempt_number, :outcome, :error, :inserted_at])
    |> validate_required([:workflow_id, :stage_name, :outcome])
    |> put_inserted_at()
  end

  defp put_inserted_at(changeset) do
    case get_field(changeset, :inserted_at) do
      nil -> put_change(changeset, :inserted_at, DateTime.utc_now())
      _ -> changeset
    end
  end
end
