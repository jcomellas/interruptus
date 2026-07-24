defmodule Interruptus.IntegrationTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.SpanStages
  alias Interruptus.Test.Support.Workflows.Suspendable
  alias Interruptus.Test.Support.Workflows.DumpFail

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    Interruptus.Test.Support.ApprovalState.reset!()
    :ok
  end

  test "start runs workflow to completion", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Simple, %{value: 4}, config: config.name)

    assert {:ok, %{status: :completed, data: %{"result" => 8}}} =
             Test.await_status(instance.id, :completed, config: config)
  end

  test "multi bare-stage span reaches checkpoint and completes", %{config: config} do
    assert {:ok, instance} = Interruptus.start(SpanStages, %{value: 5}, config: config.name)

    assert {:ok, %{status: :completed, data: data}} =
             Test.await_status(instance.id, :completed, config: config)

    assert data["a"] == 5
    assert data["b"] == 6
    assert data["c"] == 7
  end

  test "start rejects invalid params", %{config: config} do
    assert {:error, %Ecto.Changeset{}} = Interruptus.start(Simple, %{}, config: config.name)
  end

  test "corrupt persisted params fail workflow on reclaim", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => "not-a-number"},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _pid} = Interruptus.RunnerSupervisor.start_runner(config, Simple, instance.id)

    assert {:ok, %{status: :failed}} =
             Test.await_status(instance.id, :failed, config: config, timeout: 5_000)
  end

  test "invalid data dump fails workflow at checkpoint", %{config: config} do
    assert {:ok, instance} = Interruptus.start(DumpFail, %{value: 1}, config: config.name)

    assert {:ok, %{status: :failed}} =
             Test.await_status(instance.id, :failed, config: config, timeout: 5_000)
  end

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    Interruptus.Test.Support.ApprovalState.reset!()
    :ok
  end

  test "workflow suspends and resumes", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "abc"}, config: config.name)

    assert {:ok, suspended} =
             Test.await_status(instance.id, :suspended, config: config)

    assert suspended.current_stage_index == 1
    assert suspended.suspend_reason == "await_approval"
    assert suspended.suspend_metadata == %{"token" => "abc"}

    Interruptus.Test.Support.ApprovalState.approve("abc")
    assert {:ok, _pid} = Interruptus.resume(instance.id, config: config.name)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "cancel prevents restart", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :suspended,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, %{status: :cancelled}} =
             Interruptus.cancel(instance.id, config: config.name, compensate: false, force: true)
    assert {:error, :terminal} = Interruptus.resume(instance.id, config: config.name)
  end

  test "start with duplicate idempotency_key returns the existing instance", %{config: config} do
    key = "idem-#{System.unique_integer([:positive])}"

    assert {:ok, first} =
             Interruptus.start(Simple, %{value: 1}, config: config.name, idempotency_key: key)

    assert {:ok, second} =
             Interruptus.start(Simple, %{value: 99}, config: config.name, idempotency_key: key)

    assert second.id == first.id
    # Original params win; the retry did not overwrite them.
    assert second.params == %{"value" => 1}
  end

  test "non-string suspend reasons are persisted via inspect", %{config: config} do
    alias Interruptus.Test.Support.Workflows.WeirdSuspend

    assert {:ok, instance} = Interruptus.start(WeirdSuspend, %{}, config: config.name)

    assert {:ok, suspended} = Test.await_status(instance.id, :suspended, config: config)
    assert suspended.suspend_reason == ~s({:waiting_for, "partner-bank", 42})
  end

  test "pipeline_version mismatch parks the workflow as suspended", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        # Simple compiles with pipeline_version 1.
        pipeline_version: 999
      })

    assert {:ok, _pid} =
             Interruptus.RunnerSupervisor.start_runner(config, Simple, instance.id)

    assert {:ok, parked} =
             Test.await_status(instance.id, :suspended, config: config, timeout: 5_000)

    assert parked.suspend_reason == "pipeline_version_mismatch"
    assert parked.suspend_metadata == %{"stored" => 999, "compiled" => 1}
    assert parked.locked_by == nil

    # Recovery never picks it back up; stages never ran.
    :ok = Interruptus.Recovery.recover_all(config)
    refute Test.runner_pid(instance.id)
    assert {:ok, %{status: :suspended}} = Interruptus.status(instance.id, config: config.name)
  end

  test "pipeline_fingerprint mismatch parks the workflow as suspended", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        pipeline_fingerprint: "deadbeef"
      })

    assert {:ok, _pid} =
             Interruptus.RunnerSupervisor.start_runner(config, Simple, instance.id)

    assert {:ok, parked} =
             Test.await_status(instance.id, :suspended, config: config, timeout: 5_000)

    assert parked.suspend_reason == "pipeline_fingerprint_mismatch"
    assert parked.suspend_metadata["stored"] == "deadbeef"
    assert parked.suspend_metadata["compiled"] == Simple.pipeline_fingerprint()
  end
end
