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

  test "update_with_lock prevents stale writes and bumps lock_version", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, updated} = Store.update_with_lock(config, instance, %{status: :running})
    assert updated.lock_version == instance.lock_version + 1

    # The original snapshot is now fenced.
    assert {:error, :stale_lock} = Store.update_with_lock(config, instance, %{status: :failed})

    # And so is any manual attempt to keep the old version.
    assert {:ok, updated2} = Store.update_with_lock(config, updated, %{status: :running})
    assert updated2.lock_version == updated.lock_version + 1
  end

  test "update_as_holder requires holder with valid lease", %{config: config} do
    future = DateTime.add(DateTime.utc_now(), 60, :second)
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: config.node_id,
        locked_until: future
      })

    assert {:ok, updated} =
             Store.update_as_holder(config, instance, config.node_id, %{attempt_count: 1})

    assert updated.attempt_count == 1
    assert updated.lock_version == instance.lock_version + 1

    # Foreign node cannot write.
    assert {:error, :stale_lock} =
             Store.update_as_holder(config, updated, "other-node", %{attempt_count: 9})

    # Expired lease cannot write even with matching version and holder.
    {:ok, expired} =
      Store.update_with_lock(config, updated, %{locked_until: past})

    assert {:error, :stale_lock} =
             Store.update_as_holder(config, expired, config.node_id, %{attempt_count: 9})

    assert Store.get(config, instance.id).attempt_count == 1
  end

  test "list_reclaimable includes non-terminal workflows with expired lease", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    now = DateTime.utc_now()

    for status <- [:pending, :running, :compensating] do
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

  test "list_reclaimable excludes terminal, failed, and suspended statuses", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    now = DateTime.utc_now()

    for status <- [:completed, :compensated, :cancelled, :failed, :suspended] do
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
    future = DateTime.add(DateTime.utc_now(), 60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: config.node_id,
        locked_until: future
      })

    assert {:ok, updated} =
             Store.checkpoint_progress(config, instance, config.node_id, %{
               params: %{"value" => 2},
               data: %{"phase" => "done"},
               current_stage_index: 1
             })

    assert updated.current_stage_index == 1
    assert updated.params == %{"value" => 2}
    assert updated.data == %{"phase" => "done"}
    assert updated.lock_version == instance.lock_version + 1
    assert :ok = Interruptus.Test.assert_checkpoint(instance.id, 1, config: config)

    stale = %{updated | lock_version: updated.lock_version - 1}

    assert {:error, :stale_lock} =
             Store.checkpoint_progress(config, stale, config.node_id, %{current_stage_index: 2})

    # Non-holders cannot checkpoint even with the right version.
    assert {:error, :stale_lock} =
             Store.checkpoint_progress(config, updated, "other-node", %{current_stage_index: 2})
  end
end
