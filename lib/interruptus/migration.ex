defmodule Interruptus.Migration do
  @moduledoc """
  Embedded migrations for Interruptus tables.

  Creates `interruptus_workflows`, `interruptus_checkpoints`, and
  `interruptus_stage_attempts` in the host database. Host applications wrap
  these in their own `Ecto.Migration` modules:

      defmodule MyApp.Repo.Migrations.AddInterruptus do
        use Ecto.Migration

        def up, do: Interruptus.Migration.up()
        def down, do: Interruptus.Migration.down()
      end

  Use the same `:prefix` as `Interruptus.Config` when isolating tables in a
  non-public schema:

      def up, do: Interruptus.Migration.up(prefix: "private")
      def down, do: Interruptus.Migration.down(prefix: "private")
  """

  use Ecto.Migration

  @current_version 1

  @doc """
  Runs all Interruptus migrations up to the current version.

  Idempotent: skips versions already applied based on table presence.

  ## Options

    * `:version` - target version (default current library version)
    * `:prefix` - PostgreSQL schema prefix (default public schema)

  ## Returns

    * `:ok`
  """
  @spec up(keyword()) :: :ok
  def up(opts \\ []) do
    version = Keyword.get(opts, :version, @current_version)
    migrated = migrated_version(opts)

    if migrated < version do
      for v <- (migrated + 1)..version do
        apply(__MODULE__, :"up#{v}", [opts])
      end
    end

    :ok
  end

  @doc """
  Rolls back Interruptus migrations.

  ## Options

    * `:version` - target version to roll back to (default `0`, drops all tables)
    * `:prefix` - PostgreSQL schema prefix (default public schema)

  ## Returns

    * `:ok`
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) do
    version = Keyword.get(opts, :version, 0)
    migrated = migrated_version(opts)

    if migrated > version do
      for v <- migrated..(version + 1)//-1 do
        apply(__MODULE__, :"down#{v}", [opts])
      end
    end

    :ok
  end

  @spec migrated_version(keyword()) :: non_neg_integer()
  defp migrated_version(opts) do
    prefix = Keyword.get(opts, :prefix)

    if table_exists?("interruptus_workflows", prefix) do
      @current_version
    else
      0
    end
  end

  @spec table_exists?(String.t(), String.t() | nil) :: boolean()
  defp table_exists?(table, prefix) do
    Ecto.Adapters.SQL.table_exists?(repo(), table, prefix: prefix)
  rescue
    _ -> false
  end

  # Version 1 migration: creates Interruptus tables and indexes.
  @doc false
  @spec up1(keyword()) :: :ok
  def up1(opts \\ []) do
    prefix = Keyword.get(opts, :prefix)
    table_opts = [primary_key: false, prefix: prefix]

    create_if_not_exists table(:interruptus_workflows, table_opts) do
      add :id, :binary_id, primary_key: true
      add :workflow_type, :string, null: false
      add :status, :string, null: false, default: "pending"
      add :params, :map, null: false, default: "{}"
      add :data, :map, null: false, default: "{}"
      add :current_stage_index, :integer, null: false, default: 0
      add :pipeline_version, :integer, null: false, default: 1
      add :idempotency_key, :string
      add :locked_by, :string
      add :locked_until, :utc_datetime_usec
      add :lock_version, :integer, null: false, default: 0
      add :attempt_count, :integer, null: false, default: 0
      add :suspend_reason, :string
      add :suspend_metadata, :map
      add :errors, :map, null: false, default: "{}"
      timestamps(type: :utc_datetime_usec)
    end

    create_if_not_exists(
      unique_index(:interruptus_workflows, [:workflow_type, :idempotency_key],
        name: :interruptus_workflows_idempotency_idx,
        where: "idempotency_key IS NOT NULL",
        prefix: prefix
      )
    )

    create_if_not_exists(
      index(:interruptus_workflows, [:status, :locked_until],
        name: :interruptus_workflows_status_locked_idx,
        prefix: prefix
      )
    )

    create_if_not_exists table(:interruptus_checkpoints, table_opts) do
      add :id, :binary_id, primary_key: true

      add :workflow_id,
          references(:interruptus_workflows,
            type: :binary_id,
            on_delete: :delete_all,
            prefix: prefix
          ),
          null: false

      add :stage_index, :integer, null: false
      add :params, :map, null: false, default: "{}"
      add :data, :map, null: false, default: "{}"
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create_if_not_exists(
      index(:interruptus_checkpoints, [:workflow_id, :stage_index],
        name: :interruptus_checkpoints_workflow_idx,
        prefix: prefix
      )
    )

    create_if_not_exists table(:interruptus_stage_attempts, table_opts) do
      add :id, :binary_id, primary_key: true

      add :workflow_id,
          references(:interruptus_workflows,
            type: :binary_id,
            on_delete: :delete_all,
            prefix: prefix
          ),
          null: false

      add :stage_name, :string, null: false
      add :attempt_number, :integer, null: false, default: 1
      add :outcome, :string, null: false
      add :error, :map
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("now()")
    end

    create_if_not_exists(
      index(:interruptus_stage_attempts, [:workflow_id],
        name: :interruptus_stage_attempts_workflow_idx,
        prefix: prefix
      )
    )

    :ok
  end

  # Version 1 rollback: drops all Interruptus tables.
  @doc false
  @spec down1(keyword()) :: :ok
  def down1(opts \\ []) do
    prefix = Keyword.get(opts, :prefix)

    drop_if_exists table(:interruptus_stage_attempts, prefix: prefix)
    drop_if_exists table(:interruptus_checkpoints, prefix: prefix)
    drop_if_exists table(:interruptus_workflows, prefix: prefix)

    :ok
  end
end
