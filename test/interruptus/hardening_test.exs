defmodule Interruptus.HardeningTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus
  alias Interruptus.Config
  alias Interruptus.Effect
  alias Interruptus.Recovery
  alias Interruptus.Repo
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Store
  alias Interruptus.Test
  alias Interruptus.Test.Support.Barrier
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.Workflows.BarrierCompensate
  alias Interruptus.Test.Support.Workflows.DefaultsData
  alias Interruptus.Test.Support.Workflows.HaltSuccess
  alias Interruptus.Test.Support.Workflows.NoCompensate
  alias Interruptus.Test.Support.Workflows.PartialFail
  alias Interruptus.Test.Support.Workflows.Simple
  alias Interruptus.Test.Support.Workflows.SlowVerify
  alias Interruptus.Test.Support.Workflows.SuspendMutate

  setup do
    Barrier.reset!()
    CompensateOrder.reset!()
    :ok
  end

  test "cancel with compensate: true evicts a live runner and compensates", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(BarrierCompensate, %{token: "live"}, config: config.name)

    :ok = Test.await_barrier_held(:before_cancel_comp)

    assert {:ok, %{status: :compensating}} =
             Interruptus.cancel(instance.id, config: config.name, compensate: true)

    :ok = Barrier.release(:before_cancel_comp)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    assert CompensateOrder.all() == [:undo_reserve]
    assert compensated.compensation_index == 1
    assert compensated.errors["cancelled"] == "true"
  end

  test "resume of failed workflow with empty compensation plan is not_compensable",
       %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(NoCompensate, %{id: "nc1"}, config: config.name)

    assert {:ok, failed} =
             Test.await_status(instance.id, :failed, config: config, timeout: 10_000)

    assert {:error, :not_compensable} = Interruptus.resume(failed.id, config: config.name)
    assert {:ok, %{status: :failed}} = Interruptus.status(failed.id, config: config.name)
  end

  test "multi-pipeline partial failure feeds compensation the mutated command", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(PartialFail, %{id: "pf1"}, config: config.name)

    assert {:ok, compensated} =
             Test.await_status(instance.id, :compensated, config: config, timeout: 10_000)

    # Only the passed checkpoint is compensated; the in-memory/persisted
    # snapshot still carries seen=from-mutate into undo_setup.
    assert CompensateOrder.all() == [{:undo_setup, "from-mutate"}]
    assert compensated.data["seen"] == "from-mutate"
  end

  test "same-stage suspend after mutation persists data via Command.suspend/3", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(SuspendMutate, %{token: "sm"}, config: config.name)

    assert {:ok, suspended} =
             Test.await_status(instance.id, :suspended, config: config, timeout: 5_000)

    assert suspended.data["note"] == "kept"
    assert suspended.suspend_reason == "await"
  end

  test "halt(success: true) completes without compensation", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(HaltSuccess, %{id: "hs1"}, config: config.name)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 5_000)

    assert completed.data["done"] == true
    assert CompensateOrder.all() == []
  end

  test "data field defaults survive start and durable load", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(DefaultsData, %{}, config: config.name)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 5_000)

    assert completed.data["flag"] == true
    assert completed.data["label"] == "hi"
  end

  test "explicit null data overrides declared defaults on load", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.DefaultsData",
        status: :pending,
        params: %{"id" => "n1"},
        data: %{"flag" => nil, "label" => "hi"},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, _pid} = RunnerSupervisor.start_runner(config, DefaultsData, instance.id)

    assert {:ok, completed} =
             Test.await_status(instance.id, :completed, config: config, timeout: 5_000)

    # Explicit null overrode default true; omitted on dump because nil.
    refute Map.has_key?(completed.data, "flag")
    assert completed.data["label"] == "hi"
  end

  test "verify timeout is enforced by stage_timeout", %{config: config} do
    assert {:ok, instance} =
             Interruptus.start(SlowVerify, %{id: "sv1"}, config: config.name)

    assert {:ok, failed} =
             Test.await_status(instance.id, :failed, config: config, timeout: 10_000)

    assert failed.errors["failure"] =~ "timeout"
  end

  test "Config.new requires :repo", %{config: _config} do
    assert_raise ArgumentError, ~r/:repo/, fn ->
      Config.new(name: HardeningNoRepo)
    end
  end

  test "list_reclaimable keyset pagination returns all reclaimable rows", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    now = DateTime.utc_now()

    ids =
      for i <- 1..5 do
        {:ok, instance} =
          Store.insert_workflow(config, %{
            workflow_type: "Interruptus.Test.Support.Workflows.Simple",
            status: :pending,
            params: %{"value" => i},
            data: %{},
            current_stage_index: 0,
            pipeline_version: 1,
            locked_until: past
          })

        instance.id
      end

    page1 = Store.list_reclaimable(config, now, limit: 2)
    assert length(page1) == 2

    last = List.last(page1)
    page2 = Store.list_reclaimable(config, now, limit: 2, after: {last.inserted_at, last.id})
    assert length(page2) == 2

    last2 = List.last(page2)
    page3 = Store.list_reclaimable(config, now, limit: 2, after: {last2.inserted_at, last2.id})
    assert length(page3) == 1

    recovered = Enum.map(page1 ++ page2 ++ page3, & &1.id)
    assert Enum.sort(recovered) == Enum.sort(ids)
  end

  test "unknown workflow_type parking does not starve later reclaimable rows", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, poison} =
      Store.insert_workflow(config, %{
        workflow_type: "No.Such.Workflow.Module",
        status: :running,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "dead",
        locked_until: past
      })

    {:ok, good} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 2},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_until: past
      })

    :ok = Recovery.recover_all(config)

    assert {:ok, %{status: :suspended, suspend_reason: "unknown_workflow_type"}} =
             Interruptus.status(poison.id, config: config.name)

    assert {:ok, %{status: :completed}} =
             Test.await_status(good.id, :completed, config: config, timeout: 10_000)
  end

  test "status CHECK rejects invalid status values", %{config: config} do
    assert_raise Postgrex.Error, fn ->
      Repo.query!(config, """
      INSERT INTO interruptus_workflows
        (id, workflow_type, status, params, data, current_stage_index, pipeline_version,
         lock_version, attempt_count, compensation_index, errors, inserted_at, updated_at)
      VALUES
        (gen_random_uuid(), 'X', 'not_a_status', '{}', '{}', 0, 1, 0, 0, 0, '{}', now(), now())
      """)
    end
  end

  test "Effect.once returns effect_marker_failed when put cannot succeed", %{config: config} do
    command = %{Simple.new(value: 1) | workflow_id: nil}

    assert {:error, {:effect_marker_failed, :missing_workflow_id}} =
             Effect.once(command, "k", fn cmd -> Interruptus.Command.put_data(cmd, :result, 1) end, config: config.name)
  end
end
