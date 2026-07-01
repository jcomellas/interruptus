defmodule Interruptus.PolicyTest do
  use ExUnit.Case, async: false

  alias Interruptus.Policy.Restart
  alias Interruptus.Policy.Rollback
  alias Interruptus.Test.Support.CompensateOrder
  alias Interruptus.Test.Support.CompensateWorkflow

  setup_all do
    {:ok, _} = CompensateOrder.start_link()
    :ok
  end

  setup do
    CompensateOrder.reset!()
    :ok
  end

  test "restart policy backoff" do
    policy = %{backoff: :exponential, base_interval: 100, max_attempts: 5, retryable_errors: :all}
    assert Restart.retry?(policy, 1)
    refute Restart.retry?(policy, 5)
    assert Restart.backoff_ms(policy, 3) == 400
  end

  test "rollback runs compensations in LIFO order" do
    command = CompensateWorkflow.new(%{id: "1"})

    assert {:ok, _} =
             Rollback.compensate(CompensateWorkflow, command, [:compensate_a, :compensate_b])

    assert CompensateOrder.all() == [:a, :b]
  end
end
