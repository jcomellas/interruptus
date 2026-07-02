defmodule Interruptus.RunnerInterruptionTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Claim
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Barrier
  alias Interruptus.Test.Support.VerifyState
  alias Interruptus.Test.Support.Workflows.Barrier, as: BarrierWorkflow
  alias Interruptus.Test.Support.Workflows.Counting
  alias Interruptus.Test.Support.Workflows.Failing
  alias Interruptus.Test.Support.Workflows.MultiCheckpoint
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.Suspendable
  alias Interruptus.Test.Support.Workflows.VerifyFlip

  setup do
    Process.delete(:last_saved)
    Barrier.reset!()
    VerifyState.reset!()
    Interruptus.Test.Support.InvocationCounter.reset!()
    Interruptus.Test.Support.ApprovalState.reset!()
    Interruptus.Test.Support.CompensateOrder.reset!()
    :ok
  end

  test "crash at barrier with expired lease recovers to completion", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(BarrierWorkflow, %{token: "b1"}, config: config.name)

    :ok = Test.await_barrier_held(:before_checkpoint)
    :ok = Test.recover_after_interrupt(config, instance.id)
    :ok = Test.await_barrier_held(:before_checkpoint, timeout: 10_000)

    :ok = Barrier.release(:before_checkpoint)

    assert {:ok, %{status: :completed, data: %{"step" => 2}}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    assert :ok = Test.assert_checkpoint(instance.id, 0, config: config)
    assert :ok = Test.assert_checkpoint(instance.id, 2, config: config)
  end

  test "recovery re-executes side effects at least once", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Counting, %{value: 1}, config: config.name)

    :ok = Test.await_barrier_held(:in_side_effect)
    :ok = Test.recover_after_interrupt(config, instance.id)
    :ok = Test.await_barrier_held(:in_side_effect, timeout: 10_000)

    :ok = Barrier.release(:in_side_effect)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    :ok = Test.assert_invocations(:side_effect, 2)
  end

  test "split-brain stale runner does not corrupt persisted state", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(BarrierWorkflow, %{token: "split"}, config: config.name)

    :ok = Test.await_barrier_held(:before_checkpoint)

    config_b = Test.with_node_id(config, "split-brain-node-b")
    :ok = Test.expire_lease(config, instance.id)
    assert {:ok, _claimed} = Claim.acquire(config_b, instance.id)

    :ok = Barrier.release(:before_checkpoint)
    :ok = Test.recover_after_interrupt(config, instance.id)
    :ok = Test.await_barrier_held(:before_checkpoint, timeout: 10_000)
    :ok = Barrier.release(:before_checkpoint)

    assert {:ok, %{status: :completed, data: %{"step" => 2}}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 15_000)

    assert Store.get(config, instance.id).current_stage_index == 2
  end

  test "multi-checkpoint workflow resumes from correct index after crash", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(MultiCheckpoint, %{value: 1}, config: config.name)

    :ok = Test.await_barrier_held(:after_checkpoint_one)
    :ok = Test.recover_after_interrupt(config, instance.id)
    :ok = Test.await_barrier_held(:after_checkpoint_one, timeout: 10_000)

    :ok = Barrier.release(:after_checkpoint_one)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    assert completed.data["phase"] == 3
    assert :ok = Test.assert_checkpoint(instance.id, 0, config: config)
    assert :ok = Test.assert_checkpoint(instance.id, 2, config: config)
    assert :ok = Test.assert_checkpoint(instance.id, 3, config: config)
  end

  test "verify :done on resume skips checkpoint stages", %{config: config} do
    assert {:ok, instance} = Interruptus.start(VerifyFlip, %{value: 5}, config: config.name)

    :ok = Test.await_barrier_held(:before_apply)
    VerifyState.set(:done)
    :ok = Test.recover_after_interrupt(config, instance.id)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    :ok = Test.assert_invocations(:apply_result, 0)
  end

  test "verify :failed triggers compensation", %{config: config} do
    VerifyState.set(:failed)

    assert {:ok, instance} = Interruptus.start(Failing, %{id: "f1"}, config: config.name)

    assert {:ok, %{status: :compensated}} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert Interruptus.Test.Support.CompensateOrder.all() == [:compensated]
  end

  test "double resume returns the same runner pid", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "dup"}, config: config.name)

    assert {:ok, %{status: :suspended}} =
             Test.await_status(instance.id, :suspended, config: config)

    Interruptus.Test.Support.ApprovalState.approve("dup")

    assert {:ok, pid_a} = Interruptus.resume(instance.id, config: config.name)
    assert {:ok, pid_b} = Interruptus.resume(instance.id, config: config.name)
    assert pid_a == pid_b
  end

  test "cancel on running workflow with expired lease succeeds", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(BarrierWorkflow, %{token: "cancel"}, config: config.name)

    :ok = Test.await_barrier_held(:before_checkpoint)
    :ok = Test.expire_lease(config, instance.id)

    assert {:ok, %{status: :cancelled}} = Interruptus.cancel(instance.id, config: config.name)
    assert {:error, :terminal} = Interruptus.resume(instance.id, config: config.name)

    :ok = Test.crash_runner(instance.id)

    assert {:ok, %{status: :cancelled}} = Interruptus.status(instance.id, config: config.name)
  end

  test "corrupt persisted data fails workflow on reclaim", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{"result" => "not-a-number"},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _pid} = RunnerSupervisor.start_runner(config, Simple, instance.id)

    assert {:ok, %{status: :failed}} =
             Test.await_status(instance.id, :failed, config: config, timeout: 5_000)
  end
end
