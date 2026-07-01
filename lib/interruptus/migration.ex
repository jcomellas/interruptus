defmodule Interruptus.Migration do
  @moduledoc """
  Embedded migrations for Interruptus tables.

  Host applications wrap these in their own `Ecto.Migration` modules:

      def up, do: Interruptus.Migration.up()
      def down, do: Interruptus.Migration.down()
  """

  @current_version 1

  @doc """
  Runs all Interruptus migrations up to the current version.
  """
  @spec up(keyword()) :: :ok
  def up(opts \\ []) do
    version = Keyword.get(opts, :version, @current_version)
    migrated = migrated_version()

    if migrated < version do
      for v <- (migrated + 1)..version do
        apply(__MODULE__, :"up#{v}", [])
      end
    end

    :ok
  end

  @doc """
  Rolls back Interruptus migrations.
  """
  @spec down(keyword()) :: :ok
  def down(opts \\ []) do
    version = Keyword.get(opts, :version, 0)
    migrated = migrated_version()

    if migrated > version do
      for v <- migrated..(version + 1)//-1 do
        apply(__MODULE__, :"down#{v}", [])
      end
    end

    :ok
  end

  defp migrated_version do
    if table_exists?("interruptus_workflows"), do: @current_version, else: 0
  end

  defp table_exists?(table) do
    query = "SELECT to_regclass($1)"
    {:ok, %{rows: [[result]]}} = repo().query(query, [table])
    not is_nil(result)
  rescue
    _ -> false
  end

  defp repo, do: Ecto.Migration.repo()

  @doc false
  def up1 do
    repo().query!("""
    CREATE TABLE IF NOT EXISTS interruptus_workflows (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      workflow_type VARCHAR(255) NOT NULL,
      status VARCHAR(50) NOT NULL DEFAULT 'pending',
      params JSONB NOT NULL DEFAULT '{}',
      data JSONB NOT NULL DEFAULT '{}',
      current_stage_index INTEGER NOT NULL DEFAULT 0,
      pipeline_version INTEGER NOT NULL DEFAULT 1,
      idempotency_key VARCHAR(255),
      locked_by VARCHAR(255),
      locked_until TIMESTAMPTZ,
      lock_version INTEGER NOT NULL DEFAULT 0,
      attempt_count INTEGER NOT NULL DEFAULT 0,
      suspend_reason VARCHAR(255),
      suspend_metadata JSONB,
      errors JSONB NOT NULL DEFAULT '{}',
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """)

    repo().query!("""
    CREATE UNIQUE INDEX IF NOT EXISTS interruptus_workflows_idempotency_idx
    ON interruptus_workflows (workflow_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL
    """)

    repo().query!("""
    CREATE INDEX IF NOT EXISTS interruptus_workflows_status_locked_idx
    ON interruptus_workflows (status, locked_until)
    """)

    repo().query!("""
    CREATE TABLE IF NOT EXISTS interruptus_checkpoints (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      workflow_id UUID NOT NULL REFERENCES interruptus_workflows(id) ON DELETE CASCADE,
      stage_index INTEGER NOT NULL,
      params JSONB NOT NULL DEFAULT '{}',
      data JSONB NOT NULL DEFAULT '{}',
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """)

    repo().query!("""
    CREATE INDEX IF NOT EXISTS interruptus_checkpoints_workflow_idx
    ON interruptus_checkpoints (workflow_id, stage_index)
    """)

    repo().query!("""
    CREATE TABLE IF NOT EXISTS interruptus_stage_attempts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      workflow_id UUID NOT NULL REFERENCES interruptus_workflows(id) ON DELETE CASCADE,
      stage_name VARCHAR(255) NOT NULL,
      attempt_number INTEGER NOT NULL DEFAULT 1,
      outcome VARCHAR(50) NOT NULL,
      error JSONB,
      inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """)

    repo().query!("""
    CREATE INDEX IF NOT EXISTS interruptus_stage_attempts_workflow_idx
    ON interruptus_stage_attempts (workflow_id)
    """)

    :ok
  end

  @doc false
  def down1 do
    repo().query!("DROP TABLE IF EXISTS interruptus_stage_attempts")
    repo().query!("DROP TABLE IF EXISTS interruptus_checkpoints")
    repo().query!("DROP TABLE IF EXISTS interruptus_workflows")
    :ok
  end
end
