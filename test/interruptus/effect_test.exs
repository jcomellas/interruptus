defmodule Interruptus.EffectTest do
  use Interruptus.Test.Support.DataCase, async: false

  @moduletag :interruptus_integration

  alias Interruptus.Effect
  alias Interruptus.Repo
  alias Interruptus.Schemas.Effect, as: EffectSchema
  alias Interruptus.Store
  alias Interruptus.Test.Support.Workflows.Simple

  import Ecto.Query

  setup %{config: config} do
    {:ok, instance} =
      Store.insert_workflow(config, %{
        workflow_type: "Interruptus.Test.Support.Workflows.Simple",
        status: :pending,
        params: %{"value" => 1},
        data: %{},
        current_stage_index: 0,
        pipeline_version: 1,
        pipeline_fingerprint: Simple.pipeline_fingerprint()
      })

    command = %{Simple.new(value: 1) | workflow_id: instance.id}
    {:ok, instance: instance, command: command}
  end

  test "put inserts applied marker and rejects duplicates", %{config: config, command: command} do
    assert {:ok, effect} = Effect.put(command, "debit", %{ref: "d-1"}, config: config.name)
    assert effect.effect_key == "debit"
    assert effect.status == :applied
    assert effect.metadata == %{ref: "d-1"}

    assert {:error, :already_exists} =
             Effect.put(command, "debit", %{}, config: config.name)
  end

  test "key/1 joins parts for stable effect keys" do
    assert Effect.key(["debit", 42]) == "debit:42"
    assert Effect.key([:credit, "abc"]) == "credit:abc"
  end

  test "Test helpers assign workflow_id and assert applied effects",
       %{config: config, instance: instance} do
    command = Interruptus.Test.assign_workflow_id(Simple.new(value: 1), instance.id)
    key = Effect.key(["helper", instance.id])

    assert {:ok, _} = Effect.put(command, key, %{}, config: config.name)
    assert :ok = Interruptus.Test.assert_effect_applied(command, key, config: config.name)
  end

  test "exists? reflects applied markers only", %{config: config, command: command, instance: instance} do
    refute Effect.exists?(command, "credit", config: config.name)

    assert {:ok, _} =
             Effect.put(command, "credit", %{}, status: :pending, config: config.name)

    refute Effect.exists?(command, "credit", config: config.name)
    refute Effect.exists?(instance.id, "credit", config: config.name)

    assert {:ok, _} = Effect.put(command, "credit-done", %{}, config: config.name)
    assert Effect.exists?(command, "credit-done", config: config.name)
  end

  test "once claims, runs fun once, and skips on replay", %{config: config, command: command} do
    counter = :counters.new(1, [])

    fun = fn cmd ->
      :counters.add(counter, 1, 1)
      Interruptus.Command.put_data(cmd, :result, 42)
    end

    updated = Effect.once(command, "side-effect", fun, config: config.name)
    assert updated.data.result == 42
    assert :counters.get(counter, 1) == 1
    assert Effect.exists?(command, "side-effect", config: config.name)

    skipped = Effect.once(updated, "side-effect", fun, config: config.name)
    assert skipped.data.result == 42
    assert :counters.get(counter, 1) == 1
  end

  test "once does not leave marker when the stage halts", %{config: config, command: command} do
    counter = :counters.new(1, [])

    fun = fn cmd ->
      :counters.add(counter, 1, 1)
      Interruptus.Command.halt(cmd)
    end

    halted = Effect.once(command, "halted-effect", fun, config: config.name)
    assert halted.halted
    refute Effect.exists?(command, "halted-effect", config: config.name)

    _ = Effect.once(command, "halted-effect", fun, config: config.name)
    assert :counters.get(counter, 1) == 2
  end

  test "once does not leave marker on suspend", %{config: config, command: command} do
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

  test "once returns effect_in_progress for fresh pending claim",
       %{config: config, command: command} do
    assert {:ok, _} =
             Effect.put(command, "busy", %{}, status: :pending, config: config.name)

    assert {:error, :effect_in_progress, _} =
             Effect.once(command, "busy", fn cmd -> cmd end, config: config.name)
  end

  test "once reclaims stale pending markers", %{config: config, command: command, instance: instance} do
    past = DateTime.add(DateTime.utc_now(), -120_000, :millisecond)

    assert {:ok, _} =
             Effect.put(command, "stale", %{}, status: :pending, config: config.name)

    {1, _} =
      Repo.update_all(
        config,
        from(e in EffectSchema,
          where: e.workflow_id == ^instance.id and e.effect_key == "stale"
        ),
        set: [updated_at: past]
      )

    updated =
      Effect.once(
        command,
        "stale",
        fn cmd -> Interruptus.Command.put_data(cmd, :result, 7) end,
        config: config.name,
        stale_after: 30_000
      )

    assert updated.data.result == 7
    assert Effect.exists?(command, "stale", config: config.name)
  end

  test "put without workflow_id returns missing_workflow_id", %{config: config} do
    command = Simple.new(value: 1)
    assert {:error, :missing_workflow_id} = Effect.put(command, "x", %{}, config: config.name)
  end

  test "once without workflow_id returns effect_marker_failed 3-tuple", %{config: config} do
    command = %{Simple.new(value: 1) | workflow_id: nil}

    assert {:error, {:effect_marker_failed, :missing_workflow_id}, _} =
             Effect.once(command, "k", fn cmd -> Interruptus.Command.put_data(cmd, :result, 1) end,
               config: config.name
             )
  end
end
