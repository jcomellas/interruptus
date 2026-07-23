defmodule Interruptus.SuspensionTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Barrier, as: BarrierGate
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.Workflows.ApprovalComp
  alias Interruptus.Test.Support.Workflows.Barrier, as: BarrierWorkflow
  alias Interruptus.Test.Support.Workflows.Suspendable

  setup do
    BarrierGate.reset!()
    CompensateOrder.reset!()
    Interruptus.Test.Support.ApprovalState.reset!()
    :ok
  end

  test "suspended workflow stays suspended with the recovery scheduler enabled" do
    # A dedicated named instance with periodic recovery scans ENABLED —
    # the scheduler must never auto-resume a suspended workflow.
    name = SuspensionSchedulerInterruptus

    start_supervised!(
      {Interruptus,
       name: name,
       repo: Interruptus.Test.Repo,
       node_id: "scheduler-node-#{System.unique_integer([:positive])}",
       lease_duration: 5_000,
       heartbeat_interval: 2_000,
       recovery_interval: 200,
       recovery_schedule: true}
    )

    config = Interruptus.Config.fetch(name)

    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "sched"}, config: name)

    assert {:ok, %{status: :suspended}} =
             Test.await_status(instance.id, :suspended, config: config)

    # Wait through multiple scan intervals (with jitter, >5 scans).
    Process.sleep(1_500)

    assert {:ok, %{status: :suspended}} = Interruptus.status(instance.id, config: name)
    assert Test.runner_pid(instance.id, config: name) == nil

    # Explicit resume is the only way back.
    Interruptus.Test.Support.ApprovalState.approve("sched")
    assert {:ok, _pid} = Interruptus.resume(instance.id, config: name)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "resume performs a fenced suspended -> pending transition", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "fence"}, config: config.name)

    assert {:ok, suspended} = Test.await_status(instance.id, :suspended, config: config)
    assert suspended.suspend_reason == "await_approval"

    Interruptus.Test.Support.ApprovalState.approve("fence")
    assert {:ok, _pid} = Interruptus.resume(instance.id, config: config.name)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    # The resume transition bumped the fencing token and cleared suspend fields.
    assert completed.lock_version > suspended.lock_version
    assert completed.suspend_reason == nil
    assert completed.suspend_metadata == nil
  end

  test "cancel fences a live runner holding a valid lease", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(BarrierWorkflow, %{token: "live-cancel"}, config: config.name)

    # Runner is mid-stage with a perfectly valid lease.
    :ok = Test.await_barrier_held(:before_checkpoint)
    runner = Test.runner_pid(instance.id)
    assert is_pid(runner)
    ref = Process.monitor(runner)

    row = Store.get(config, instance.id)
    assert row.locked_by == config.node_id
    assert DateTime.compare(row.locked_until, DateTime.utc_now()) == :gt

    assert {:ok, %{status: :cancelled}} = Interruptus.cancel(instance.id, config: config.name)

    # Let the in-flight stage finish; the runner's next fenced write must fail
    # and the runner must stop without resurrecting the workflow.
    :ok = BarrierGate.release(:before_checkpoint)

    assert_receive {:DOWN, ^ref, :process, ^runner, reason}, 5_000
    assert reason in [:normal, :lease_lost]

    row = Store.get(config, instance.id)
    assert row.status == :cancelled
    # The checkpoint after the barrier never landed: no progress was persisted
    # by the fenced runner after cancellation.
    assert row.data == %{}
    assert row.current_stage_index == 0

    assert {:error, :terminal} = Interruptus.resume(instance.id, config: config.name)
    assert {:error, :terminal} = Interruptus.cancel(instance.id, config: config.name)
  end

  test "cancel with compensate: true rolls back passed checkpoints", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(ApprovalComp, %{token: "comp-cancel"}, config: config.name)

    # Suspends at the gate after passing the compensable :reserve checkpoint.
    assert {:ok, suspended} = Test.await_status(instance.id, :suspended, config: config)
    assert suspended.current_stage_index == 1

    assert {:ok, %{status: :compensating}} =
             Interruptus.cancel(instance.id, config: config.name, compensate: true)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert CompensateOrder.all() == [:undo_reserve]
    assert compensated.compensation_index == 1
    assert compensated.errors["cancelled"] == "true"

    assert {:error, :terminal} = Interruptus.resume(instance.id, config: config.name)
  end

  test "cancel with compensate: true and no passed checkpoints cancels plainly",
       %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.ApprovalComp",
        status: :suspended,
        params: %{"token" => "nothing-done"},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, %{status: :cancelled}} =
             Interruptus.cancel(instance.id, config: config.name, compensate: true)

    assert CompensateOrder.all() == []
  end

  test "suspend persists progress and metadata", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "meta"}, config: config.name)

    assert {:ok, suspended} = Test.await_status(instance.id, :suspended, config: config)

    assert suspended.current_stage_index == 1
    assert suspended.suspend_reason == "await_approval"
    assert suspended.suspend_metadata == %{"token" => "meta"}
    assert suspended.data == %{"step" => 1}
    assert suspended.locked_by == nil
    assert suspended.locked_until == nil
    # Attempt budget resets on suspension: resume starts fresh.
    assert suspended.attempt_count == 0
  end
end
