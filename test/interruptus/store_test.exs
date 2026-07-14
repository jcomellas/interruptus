defmodule Interruptus.StoreTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

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

  test "list_reclaimable includes non-terminal workflows with expired lease", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    now = DateTime.utc_now()

    for status <- [:pending, :suspended, :running] do
      {:ok, instance} =
        Store.insert_workflow(config, %{
          workflow_type: "Test",
          status: status,
          params: %{},
          data: %{},
          current_stage_index: 0,
          pipeline_version: 1,
          locked_until: past
        })

      assert instance.id in Enum.map(Store.list_reclaimable(config, now), & &1.id)
    end

    {:ok, pending_nil} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_until: nil
      })

    assert pending_nil.id in Enum.map(Store.list_reclaimable(config, now), & &1.id)
  end

  test "list_reclaimable excludes terminal and failed statuses", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    now = DateTime.utc_now()

    for status <- [:completed, :compensated, :cancelled, :failed, :compensating] do
      {:ok, instance} =
        Store.insert_workflow(config, %{
          workflow_type: "Test",
          status: status,
          params: %{},
          data: %{},
          current_stage_index: 0,
          pipeline_version: 1,
          locked_until: past
        })

      refute instance.id in Enum.map(Store.list_reclaimable(config, now), & &1.id)
    end
  end

  test "write_checkpoint appends multiple rows", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _} =
             Store.write_checkpoint(config, %{instance | current_stage_index: 1, data: %{"phase" => 1}})

    assert {:ok, _} =
             Store.write_checkpoint(config, %{instance | current_stage_index: 2, data: %{"phase" => 2}})

    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 0, config: config)
    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 1, config: config)
    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 2, config: config)
  end

  test "checkpoint_progress updates row and writes audit atomically", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, updated} =
             Store.checkpoint_progress(config, instance, %{
               params: %{"value" => 2},
               data: %{"phase" => "done"},
               current_stage_index: 1
             })

    assert updated.current_stage_index == 1
    assert updated.params == %{"value" => 2}
    assert updated.data == %{"phase" => "done"}
    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 1, config: config)

    stale = %{updated | lock_version: updated.lock_version - 1}

    assert {:error, :stale_lock} =
             Store.checkpoint_progress(config, stale, %{current_stage_index: 2})
  end
end
