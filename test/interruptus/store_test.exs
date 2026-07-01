defmodule Interruptus.StoreTest do
  use Interruptus.Test.Support.DataCase, async: false

  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  test "insert_workflow creates instance and initial checkpoint", %{config: config} do
    assert {:ok, instance} =
             Store.insert_workflow(config, %{
               workflow_type: "Interruptus.Test.Support.Workflows.Simple",
               status: :pending,
               params: %{"value" => 1},
               data: %{},
               current_stage_index: 0,
               pipeline_version: 1
             })

    assert %WorkflowInstance{status: :pending} = Store.get(config, instance.id)
    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 0, config: config)
  end

  test "update_with_lock prevents stale writes", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _} =
             Store.update_with_lock(config, instance, %{
               status: :running,
               lock_version: instance.lock_version + 1
             })

    assert {:error, :stale_lock} = Store.update_with_lock(config, instance, %{status: :failed})
  end
end
