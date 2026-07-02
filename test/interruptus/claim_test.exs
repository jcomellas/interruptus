defmodule Interruptus.ClaimTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus.Claim
  alias Interruptus.Store
  alias Interruptus.Test

  test "acquire grants exclusive lease", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, claimed} = Claim.acquire(config, instance.id)
    assert claimed.status == :running
    assert claimed.locked_by == config.node_id

    other_config = %{config | node_id: "other-node"}
    assert {:error, :not_claimable} = Claim.acquire(other_config, instance.id)
  end

  test "stale lease can be reclaimed", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "dead-node",
        locked_until: past,
        lock_version: 1
      })

    other_config = %{config | node_id: "new-node"}
    assert {:ok, claimed} = Claim.acquire(other_config, instance.id)
    assert claimed.locked_by == "new-node"
  end

  test "acquire on terminal status returns not_claimable", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :completed,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:error, :not_claimable} = Claim.acquire(config, instance.id)
  end

  test "acquire on missing id returns not_found", %{config: config} do
    assert {:error, :not_found} = Claim.acquire(config, Ecto.UUID.generate())
  end

  test "renew returns not_holder for foreign lease", %{config: config} do
    past = DateTime.add(DateTime.utc_now(), -60, :second)

    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :running,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        locked_by: "other-node",
        locked_until: past,
        lock_version: 1
      })

    assert {:error, :not_holder} = Claim.renew(config, instance)
  end

  test "renew returns stale_lock after version bump", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, claimed} = Claim.acquire(config, instance.id)
    stale = %{claimed | lock_version: claimed.lock_version - 1}
    assert {:error, :stale_lock} = Claim.renew(config, stale)
  end

  test "release clears lease fields", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:ok, claimed} = Claim.acquire(config, instance.id)
    assert {:ok, released} = Claim.release(config, claimed)
    assert released.locked_by == nil
    assert released.locked_until == nil
  end

  test "lock_version increments only on acquire and never decreases", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert Store.get(config, instance.id).lock_version == 0

    assert {:ok, claimed} = Claim.acquire(config, instance.id)
    assert claimed.lock_version == 1
    assert Store.get(config, instance.id).lock_version == 1

    assert {:ok, renewed} = Claim.renew(config, claimed)
    assert renewed.lock_version == 1
    assert Store.get(config, instance.id).lock_version == 1

    assert {:ok, released} = Claim.release(config, renewed)
    assert released.lock_version == 1
    assert Store.get(config, instance.id).lock_version == 1

    assert {:ok, reclaimed} = Claim.acquire(config, instance.id)
    assert reclaimed.lock_version == 2
    assert Store.get(config, instance.id).lock_version == 2
  end

  test "re-acquire by another node bumps lock_version and fences stale renew", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    config_a = config
    config_b = Test.with_node_id(config, "fencing-node-b")

    assert {:ok, claimed_a} = Claim.acquire(config_a, instance.id)
    assert claimed_a.lock_version == 1
    stale_a = claimed_a

    :ok = Test.expire_lease(config_a, instance.id)
    assert {:ok, claimed_b} = Claim.acquire(config_b, instance.id)
    assert claimed_b.lock_version == 2

    assert {:error, :stale_lock} = Claim.renew(config_a, stale_a)

    row = Store.get(config, instance.id)
    assert row.lock_version == 2
    assert row.locked_by == config_b.node_id
  end

  test "parallel acquire has exactly one winner", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    config_a = config
    config_b = Test.with_node_id(config, "race-node-b")

    [result_a, result_b] = Test.race_acquire(config_a, config_b, instance.id)

    assert Enum.count([result_a, result_b], &match?({:ok, _}, &1)) == 1
    assert Enum.count([result_a, result_b], &(&1 == {:error, :not_claimable})) == 1
  end
end
