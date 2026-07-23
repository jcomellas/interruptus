defmodule Interruptus.CompensationTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Policy.Rollback
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Barrier, as: BarrierGate
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.InvocationCounter
  alias Interruptus.Test.Support.Workflows.CompCrash
  alias Interruptus.Test.Support.Workflows.Raising

  setup do
    BarrierGate.reset!()
    CompensateOrder.reset!()
    InvocationCounter.reset!()
    :ok
  end

  describe "compensation_plan/2" do
    test "scopes per-checkpoint compensations to passed checkpoints, LIFO" do
      # Segment layout: 0 = ckpt(undo_one), 1 = ckpt(undo_two),
      # 2 = ckpt(no compensate), 3 = ckpt(undo_never)
      assert Rollback.compensation_plan(CompCrash, 0) == []
      assert Rollback.compensation_plan(CompCrash, 1) == [:undo_one]
      assert Rollback.compensation_plan(CompCrash, 2) == [:undo_two, :undo_one]
      assert Rollback.compensation_plan(CompCrash, 3) == [:undo_two, :undo_one]
      assert Rollback.compensation_plan(CompCrash, 4) == [:undo_never, :undo_two, :undo_one]
    end

    test "appends the workflow-level rollback list after checkpoint compensations" do
      # Raising: segment 0 = ckpt(undo_first), 1 = ckpt(boom, no compensate);
      # workflow-level list: [:final_cleanup].
      assert Rollback.compensation_plan(Raising, 0) == [:final_cleanup]
      assert Rollback.compensation_plan(Raising, 1) == [:undo_first, :final_cleanup]
    end
  end

  test "crash mid-compensation resumes from the persisted compensation_index",
       %{config: config} do
    assert {:ok, instance} = Interruptus.start(CompCrash, %{token: "cc"}, config: config.name)

    # Forward execution passes checkpoints 0 and 1, then :always_halts fails
    # (3 attempts), and compensation starts with :undo_two gated on a barrier.
    :ok = Test.await_barrier_held(:in_undo_two, timeout: 10_000)

    row = Store.get(config, instance.id)
    assert row.status == :compensating
    assert row.compensation_index == 0
    assert row.current_stage_index == 2

    # Crash the runner mid-compensation and reclaim after lease expiry.
    :ok = Test.recover_after_interrupt(config, instance.id)

    # The reclaimed runner continues compensating: undo_two re-runs
    # (at-least-once), gated again.
    :ok = Test.await_barrier_held(:in_undo_two, timeout: 10_000)
    :ok = BarrierGate.release(:in_undo_two)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert compensated.compensation_index == 2

    order = Enum.reverse(CompensateOrder.all())
    # undo_two completed once (the first run was killed at the barrier),
    # then undo_one. Compensations for the unreached checkpoint never ran.
    assert order == [:undo_two, :undo_one]
    refute :undo_never in order
  end

  test "compensation steps persist progress so completed steps never re-run",
       %{config: config} do
    assert {:ok, instance} = Interruptus.start(CompCrash, %{token: "cc2"}, config: config.name)

    :ok = Test.await_barrier_held(:in_undo_two, timeout: 10_000)
    :ok = BarrierGate.release(:in_undo_two)

    # Wait until undo_two's completion is durably recorded.
    :ok = await_compensation_index(config, instance.id, 1)

    # Crash after the first compensation step landed.
    :ok = Test.recover_after_interrupt(config, instance.id)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert compensated.compensation_index == 2

    order = Enum.reverse(CompensateOrder.all())
    # undo_two ran exactly once: its persisted index fenced it from re-running.
    assert order == [:undo_two, :undo_one]
  end

  test "reclaim of a :compensating workflow preserves its status", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Raising",
        status: :compensating,
        params: %{"id" => "resumed-comp"},
        data: %{"step" => 1},
        current_stage_index: 1,
        pipeline_version: 1,
        compensation_index: 1,
        locked_by: "dead-node",
        locked_until: past
      })

    :ok = Interruptus.Recovery.recover_all(config)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    # Plan for index 1 is [:undo_first, :final_cleanup]; index 1 was already
    # done, so only :final_cleanup runs.
    assert compensated.compensation_index == 2
    assert CompensateOrder.all() == [:final_cleanup]
  end

  test "resume retries compensation for :failed workflows", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Raising",
        status: :failed,
        params: %{"id" => "failed-comp"},
        data: %{"step" => 1},
        current_stage_index: 1,
        pipeline_version: 1,
        compensation_index: 0,
        attempt_count: 2,
        errors: %{"failure" => "compensation_exhausted"}
      })

    assert {:ok, _pid} = Interruptus.resume(instance.id, config: config.name)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert compensated.compensation_index == 2
    assert Enum.reverse(CompensateOrder.all()) == [:undo_first, :final_cleanup]
  end

  test "failure with no compensations marks the workflow :failed", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    # CompCrash at index 0: nothing passed, empty plan -> :failed, not
    # :compensated (nothing was rolled back).
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.CompCrash",
        status: :running,
        params: %{"token" => "no-comp"},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        attempt_count: 3,
        locked_by: "dead-node",
        locked_until: past
      })

    assert {:ok, _pid} = RunnerSupervisor.start_runner(config, CompCrash, instance.id)

    assert {:ok, failed} =
             Test.await_status(instance.id, :failed, config: config, timeout: 10_000)

    assert failed.errors["failure"] =~ "attempts_exhausted"
    assert CompensateOrder.all() == []
  end

  defp await_compensation_index(config, workflow_id, index) do
    deadline = System.monotonic_time(:millisecond) + 10_000
    do_await_compensation_index(config, workflow_id, index, deadline)
  end

  defp do_await_compensation_index(config, workflow_id, index, deadline) do
    row = Store.get(config, workflow_id)

    cond do
      row && row.compensation_index >= index ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :timeout}

      true ->
        Process.sleep(25)
        do_await_compensation_index(config, workflow_id, index, deadline)
    end
  end
end
