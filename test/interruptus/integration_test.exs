defmodule Interruptus.IntegrationTest do
  use Interruptus.Test.Support.DataCase, async: false

  alias Interruptus
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.Suspendable
  alias Interruptus.Test.Support.Workflows.DumpFail

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    :ok
  end

  setup_all do
    {:ok, _} = Interruptus.Test.Support.ApprovalState.start_link()
    :ok
  end

  test "start runs workflow to completion", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Simple, %{value: 4}, config: config.name)

    assert {:ok, %{status: :completed, data: %{"result" => 8}}} =
             Test.await_status(instance.id, :completed, config: config)
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

    assert {:ok, %{status: :suspended}} =
             Test.await_status(instance.id, :suspended, config: config)

    Interruptus.Test.Support.ApprovalState.approve("abc")
    assert {:ok, _pid} = Interruptus.resume(instance.id, config: config.name)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "crash recovery continues from checkpoint", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Simple, %{value: 2}, config: config.name)

    # Wait until running then crash
    Process.sleep(50)
    :ok = Test.crash_runner(instance.id)

    Interruptus.Recovery.recover_all(config)

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

    assert {:ok, %{status: :cancelled}} = Interruptus.cancel(instance.id, config: config.name)
    assert {:error, :terminal} = Interruptus.resume(instance.id, config: config.name)
  end
end
