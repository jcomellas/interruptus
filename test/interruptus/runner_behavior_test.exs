defmodule Interruptus.RunnerBehaviorTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Claim
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.InvocationCounter
  alias Interruptus.Test.Support.Workflows.BadReturn
  alias Interruptus.Test.Support.Workflows.Flaky
  alias Interruptus.Test.Support.Workflows.LongSlow
  alias Interruptus.Test.Support.Workflows.Raising
  alias Interruptus.Test.Support.Workflows.TimedOut

  setup do
    InvocationCounter.reset!()
    CompensateOrder.reset!()
    Interruptus.Test.Support.Barrier.reset!()
    :ok
  end

  test "failing stage retries through the runner and succeeds", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(Flaky, %{succeed_on_attempt: 3}, config: config.name)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    assert completed.data["result"] == 42
    # Two halted attempts + one success.
    assert :ok = Test.assert_invocations(:flaky_stage, 3)
    # Budget reset on the successful checkpoint.
    assert completed.attempt_count == 0
    assert CompensateOrder.all() == []
  end

  test "attempt_count is persisted before execution", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(Flaky, %{succeed_on_attempt: 99}, config: config.name)

    # While attempts are burning down, the persisted attempt_count must always
    # be >= the number of started executions minus the in-flight one, and the
    # workflow must end compensated after max_attempts (5), not loop forever.
    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert :ok = Test.assert_invocations(:flaky_stage, 5)
    assert compensated.compensation_index == 1
    assert CompensateOrder.all() == [:flaky_undo]
    assert compensated.errors["failure"] =~ "halted"
  end

  test "raising stage is bounded by restart policy and ends compensated", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Raising, %{id: "r1"}, config: config.name)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    # max_attempts: 2 — the raise must not crash-loop the runner.
    assert :ok = Test.assert_invocations(:boom, 2)

    # Passed checkpoint compensation (LIFO) then workflow-level list.
    assert Enum.reverse(CompensateOrder.all()) == [:undo_first, :final_cleanup]
    assert compensated.compensation_index == 2
    assert compensated.errors["failure"] =~ "boom stage always raises"

    # No runner left behind.
    assert await_no_runner(instance.id)
  end

  test "invalid stage return value fails through policy instead of crashing", %{config: config} do
    assert {:ok, instance} = Interruptus.start(BadReturn, %{}, config: config.name)

    # max_attempts: 1, no compensations -> :failed with the invalid result recorded.
    assert {:ok, failed} =
             Test.await_status(instance.id, :failed, config: config, timeout: 10_000)

    assert failed.errors["failure"] =~ "invalid_stage_result"
  end

  test "heartbeat keeps lease alive during a stage longer than lease_duration", %{config: config} do
    fast_config = %{config | lease_duration: 400, heartbeat_interval: 100}

    {:ok, instance} =
      Store.insert_workflow(fast_config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.LongSlow",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _pid} = RunnerSupervisor.start_runner(fast_config, LongSlow, instance.id)
    :ok = Test.await_runner(instance.id)

    other_node = Test.with_node_id(fast_config, "competing-node")

    # While the 1s stage runs (lease is only 400ms), the lease must stay
    # renewed and a competing node must never be able to claim.
    for _ <- 1..4 do
      Process.sleep(150)

      case Store.get(fast_config, instance.id) do
        %{status: :completed} ->
          :ok

        row ->
          assert row.locked_by == fast_config.node_id
          assert DateTime.compare(row.locked_until, DateTime.utc_now()) == :gt
          assert {:error, :not_claimable} = Claim.acquire(other_node, instance.id)
      end
    end

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: fast_config, timeout: 10_000)

    assert completed.data["done"] == true
  end

  test "stage timeout kills the task and flows through restart policy", %{config: config} do
    assert {:ok, instance} = Interruptus.start(TimedOut, %{}, config: config.name)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)

    # First invocation timed out (and its task was killed), second succeeded.
    assert :ok = Test.assert_invocations(:maybe_slow, 2)
    assert completed.data["done"] == true
  end

  test "runner that cannot claim stops and frees the registry slot", %{config: config} do
    future = DateTime.add(DateTime.utc_now(), 60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :running,
        params: %{"value" => 3},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "some-other-node",
        locked_until: future
      })

    # Resume while another node holds a valid lease: the local runner starts,
    # fails to claim, and must stop instead of becoming an immortal zombie.
    assert {:ok, pid} = Interruptus.resume(instance.id, config: config.name)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000

    # Registry unregistration is asynchronous with respect to the DOWN signal.
    assert await_no_runner(instance.id)

    # The row was not touched.
    row = Store.get(config, instance.id)
    assert row.locked_by == "some-other-node"
    assert row.status == :running

    # After the lease expires, resume works with a fresh runner.
    :ok = Test.expire_lease(config, instance.id)
    assert {:ok, _pid2} = Interruptus.resume(instance.id, config: config.name)

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "exhausted attempt budget at claim goes to rollback, bounding poison pills",
       %{config: config} do
    # Simulate a workflow that already burned its budget through crashes:
    # attempt_count is at max and the lease has expired.
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Raising",
        status: :running,
        params: %{"id" => "poison"},
        data: %{"step" => 1},
        current_stage_index: 1,
        pipeline_version: 1,
        attempt_count: 2,
        locked_by: "dead-node",
        locked_until: past
      })

    assert {:ok, _pid} = RunnerSupervisor.start_runner(config, Raising, instance.id)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    # The failing stage never ran again: budget was already exhausted.
    assert :ok = Test.assert_invocations(:boom, 0)
    assert Enum.reverse(CompensateOrder.all()) == [:undo_first, :final_cleanup]
    assert compensated.errors["failure"] =~ "attempts_exhausted"
  end

  defp await_no_runner(workflow_id, deadline_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_await_no_runner(workflow_id, deadline)
  end

  defp do_await_no_runner(workflow_id, deadline) do
    cond do
      Test.runner_pid(workflow_id) == nil ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(20)
        do_await_no_runner(workflow_id, deadline)
    end
  end
end
