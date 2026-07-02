defmodule Interruptus.Schemas.StageAttempt do
  @moduledoc """
  Ecto schema for the `interruptus_stage_attempts` table.

  Logs each stage failure, halt, timeout, or verify outcome for observability
  and debugging.

  ## Fields

    * `:workflow_id` - parent workflow instance UUID
    * `:stage_name` - identifier for the stage that was attempted
    * `:attempt_number` - 1-based retry counter
    * `:outcome` - one of `:success`, `:failure`, `:suspended`, `:halted`,
      `:timeout`, `:verify_done`, `:verify_not_done`, `:verify_failed`
    * `:error` - optional JSON map with error details
    * `:inserted_at` - attempt timestamp
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id

  @type outcome ::
          :success
          | :failure
          | :suspended
          | :halted
          | :timeout
          | :verify_done
          | :verify_not_done
          | :verify_failed

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          workflow_id: Ecto.UUID.t(),
          stage_name: String.t(),
          attempt_number: integer(),
          outcome: outcome(),
          error: map() | nil,
          inserted_at: DateTime.t()
        }

  @type attempt_attrs :: %{
          required(:workflow_id) => Ecto.UUID.t(),
          required(:stage_name) => String.t(),
          required(:attempt_number) => pos_integer(),
          required(:outcome) => outcome(),
          optional(:error) => map()
        }

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

  # Builds a changeset for stage attempt insert. Used by Interruptus.Runner.
  @doc false
  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(attempt, attrs) do
    attempt
    |> cast(attrs, [:workflow_id, :stage_name, :attempt_number, :outcome, :error, :inserted_at])
    |> validate_required([:workflow_id, :stage_name, :outcome])
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
