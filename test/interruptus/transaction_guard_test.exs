defmodule Interruptus.TransactionGuardTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus.Claim
  alias Interruptus.Store
  alias Interruptus.Test.Repo
  alias Interruptus.Test.Support.Workflows.Simple

  test "start rejects nested transactions", %{config: config} do
    assert {:error, :in_transaction} =
             Repo.transaction(fn ->
               Interruptus.start(Simple, %{value: 1}, config: config.name)
             end)
             |> elem_or_error()
  end

  test "resume rejects nested transactions", %{config: config} do
    assert {:ok, instance} = Interruptus.start(Simple, %{value: 1}, config: config.name)

    assert {:error, :in_transaction} =
             Repo.transaction(fn ->
               Interruptus.resume(instance.id, config: config.name)
             end)
             |> elem_or_error()
  end

  test "cancel rejects nested transactions", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:error, :in_transaction} =
             Repo.transaction(fn ->
               Interruptus.cancel(instance.id, config: config.name)
             end)
             |> elem_or_error()
  end

  test "claim acquire rejects nested transactions", %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Test",
        status: :pending,
        params: %{},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    assert {:error, :in_transaction} =
             Repo.transaction(fn ->
               Claim.acquire(config, instance.id)
             end)
             |> elem_or_error()
  end

  defp elem_or_error({:ok, {:error, reason}}), do: {:error, reason}
  defp elem_or_error({:ok, other}), do: other
  defp elem_or_error({:error, reason}), do: {:error, reason}
end
