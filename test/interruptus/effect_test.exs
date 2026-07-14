defmodule Interruptus.EffectTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus.Effect
  alias Interruptus.Store
  alias Interruptus.Test.Support.Workflows.Simple

  setup %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1
      })

    command = %{Simple.new(value: 1) | workflow_id: instance.id}
    {:ok, instance: instance, command: command}
  end

  test "put inserts marker and rejects duplicates", %{config: config, command: command} do
    assert {:ok, effect} = Effect.put(command, "debit", %{ref: "d-1"}, config: config.name)
    assert effect.effect_key == "debit"
    assert effect.metadata == %{ref: "d-1"}

    assert {:error, :already_exists} =
             Effect.put(command, "debit", %{}, config: config.name)
  end

  test "exists? reflects markers", %{config: config, command: command, instance: instance} do
    refute Effect.exists?(command, "credit", config: config.name)
    refute Effect.exists?(instance.id, "credit", config: config.name)

    assert {:ok, _} = Effect.put(command, "credit", %{}, config: config.name)

    assert Effect.exists?(command, "credit", config: config.name)
    assert Effect.exists?(instance.id, "credit", config: config.name)
  end

  test "once runs fun once and skips on replay", %{config: config, command: command} do
    counter = :counters.new(1, [])

    fun = fn cmd ->
      :counters.add(counter, 1, 1)
      Interruptus.Command.put_data(cmd, :result, 42)
    end

    updated = Effect.once(command, "side-effect", fun, config: config.name)
    assert updated.data.result == 42
    assert :counters.get(counter, 1) == 1

    skipped = Effect.once(updated, "side-effect", fun, config: config.name)
    assert skipped.data.result == 42
    assert :counters.get(counter, 1) == 1
  end

  test "once does not record marker on suspend", %{config: config, command: command} do
    result =
      Effect.once(
        command,
        "await",
        fn _cmd -> {:suspend, :wait, %{}} end,
        config: config.name
      )

    assert result == {:suspend, :wait, %{}}
    refute Effect.exists?(command, "await", config: config.name)
  end

  test "put without workflow_id returns missing_workflow_id", %{config: config} do
    command = Simple.new(value: 1)
    assert {:error, :missing_workflow_id} = Effect.put(command, "x", %{}, config: config.name)
  end
end
