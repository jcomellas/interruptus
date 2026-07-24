defmodule Interruptus.PurgeTest do
  use Interruptus.Test.Support.DataCase, async: false

  import Ecto.Query

  @moduletag :interruptus_integration

  alias Interruptus.Store
  alias Interruptus.Test.Support.Workflows.Simple

  test "purge_terminal deletes old terminal rows only", %{config: config} do
    old = DateTime.add(DateTime.utc_now(), -86_400, :second)

    {:ok, completed} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :completed,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 2,
        pipeline_version: 1,
        pipeline_fingerprint: Simple.pipeline_fingerprint()
      })

    {:ok, suspended} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :suspended,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        pipeline_fingerprint: Simple.pipeline_fingerprint()
      })

    # Backdate the completed row so it falls inside the retention window.
    {1, _} =
      Interruptus.Repo.update_all(
        config,
        from(w in Interruptus.Schemas.WorkflowInstance, where: w.id == ^completed.id),
        set: [updated_at: old]
      )

    assert {:ok, 1} =
             Interruptus.purge_terminal(config: config.name, older_than: 3_600_000)

    assert Store.get(config, completed.id) == nil
    assert Store.get(config, suspended.id)

    # Defaults leave Recovery purge off.
    assert config.purge_schedule == false
    assert config.retention_ms == nil
  end
end
