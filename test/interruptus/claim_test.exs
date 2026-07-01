defmodule Interruptus.ClaimTest do
  use Interruptus.Test.Support.DataCase, async: false

  alias Interruptus.Claim
  alias Interruptus.Store

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
end
