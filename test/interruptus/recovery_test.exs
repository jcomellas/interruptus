defmodule Interruptus.RecoveryTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Recovery
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.Suspendable

  setup do
    Process.delete(:last_saved)
    Process.put(:verify_result, :not_done)
    Interruptus.Test.Support.ApprovalState.reset!()
    :ok
  end

  test "recover_all reclaims expired running workflow", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :running,
        params: %{"value" => 3},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "dead-node",
        locked_until: past,
        lock_version: 1
      })

    :ok = Recovery.recover_all(config)
    assert is_pid(Test.runner_pid(instance.id))

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "recover_all ignores terminal workflows", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :completed,
        params: %{"value" => 1},
        data: %{"result" => 2},
        current_stage_index: 2,
        pipeline_version: 1,
        locked_until: past
      })

    :ok = Recovery.recover_all(config)
    refute Test.runner_pid(instance.id)
  end

  test "concurrent recover_all starts a single runner", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :running,
        params: %{"value" => 2},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "dead-node",
        locked_until: past,
        lock_version: 1
      })

    parent = self()
    repo = Interruptus.Test.Repo

    tasks =
      for _ <- 1..5 do
        Task.async(fn ->
          Ecto.Adapters.SQL.Sandbox.allow(repo, parent, self())
          Recovery.recover_all(config)
        end)
      end

    Enum.each(tasks, &Task.await/1)

    assert is_pid(Test.runner_pid(instance.id))

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "resume and recover_all race leaves one runner and consistent status", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Suspendable, %{token: "race"}, config: config.name)

    assert {:ok, %{status: :suspended}} =
             Test.await_status(instance.id, :suspended, config: config)

    Interruptus.Test.Support.ApprovalState.approve("race")

    parent = self()
    repo = Interruptus.Test.Repo

    resume_task =
      Task.async(fn ->
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, self())
        Interruptus.resume(instance.id, config: config.name)
      end)

    recover_task =
      Task.async(fn ->
        Ecto.Adapters.SQL.Sandbox.allow(repo, parent, self())
        Recovery.recover_all(config)
      end)

    assert {:ok, pid_a} = Task.await(resume_task)
    assert :ok = Task.await(recover_task)

    # Either the resumed runner is still registered (same pid) or it already
    # completed and deregistered; a second concurrent runner must never exist.
    case Test.runner_pid(instance.id) do
      nil -> :ok
      pid_b -> assert pid_a == pid_b
    end

    assert {:ok, %{status: :completed}} =
             Test.await_status(instance.id, :completed, config: config, timeout: 10_000)
  end

  test "recover_all never reclaims suspended workflows", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Suspendable",
        status: :suspended,
        params: %{"token" => "parked"},
        data: %{"step" => 1},
        current_stage_index: 1,
        pipeline_version: 1,
        locked_until: past,
        suspend_reason: "await_approval"
      })

    :ok = Recovery.recover_all(config)
    refute Test.runner_pid(instance.id)
    assert {:ok, %{status: :suspended}} = Interruptus.status(instance.id, config: config.name)
  end

  test "recover_all skips unresolvable workflow types without mutating them", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    handler_id = "unknown-type-#{System.unique_integer()}"
    parent = self()

    :telemetry.attach(
      handler_id,
      [:interruptus, :recovery, :unknown_workflow_type],
      fn _event, _measurements, metadata, _ ->
        send(parent, {:unknown_type, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "No.Such.Workflow.Module",
        status: :running,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "dead-node",
        locked_until: past
      })

    :ok = Recovery.recover_all(config)

    assert_receive {:unknown_type, %{workflow_type: "No.Such.Workflow.Module"}}, 1_000
    refute Test.runner_pid(instance.id)

    # Row untouched: a node running newer code can still recover it.
    row = Store.get(config, instance.id)
    assert row.status == :running
    assert row.lock_version == instance.lock_version
  end

  test "start_runner returns existing pid when runner already registered", %{config: config} do
    {:ok, instance} = Interruptus.start(Simple, %{value: 1}, config: config.name)
    :ok = Test.await_runner(instance.id)

    assert {:ok, pid_a} = RunnerSupervisor.start_runner(config, Simple, instance.id)
    assert {:ok, pid_b} = RunnerSupervisor.start_runner(config, Simple, instance.id)
    assert pid_a == pid_b
  end
end
