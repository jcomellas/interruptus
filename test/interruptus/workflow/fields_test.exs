defmodule Interruptus.Workflow.FieldsTest do
  use ExUnit.Case, async: true

  alias Interruptus.Test.Support.Workflows.TypedFields
  alias Interruptus.Type.Decimal, as: DecimalType
  alias Interruptus.Workflow.CastError

  describe "cast_params/1" do
    test "casts string input to typed values" do
      assert {:ok, params} =
               TypedFields.cast_params(%{
                 "required_int" => "5",
                 "amount" => "12.50"
               })

      assert params.required_int == 5
      assert params.optional_int == 10
      assert params.amount == Decimal.new("12.50")
    end

    test "returns changeset error when required param is missing" do
      assert {:error, %Ecto.Changeset{}} = TypedFields.cast_params(%{amount: "1"})
    end
  end

  describe "dump and load round-trip" do
    test "dump_params omits nil optional fields" do
      assert {:ok, dumped} =
               TypedFields.dump_params(%{required_int: 1, optional_int: nil, amount: Decimal.new("3")})

      assert dumped == %{"required_int" => 1, "amount" => "3"}
    end

    test "dump_data omits nil fields and validates dump-then-cast" do
      assert {:ok, dumped} = TypedFields.dump_data(%{name: "alice", count: nil})
      assert dumped == %{"name" => "alice"}

      assert {:ok, loaded} = TypedFields.load_data(dumped)
      assert loaded.name == "alice"
      assert loaded.count == nil
    end

    test "dump_data rejects values that fail dump validation" do
      assert {:error, %CastError{field: :name, operation: :dump}} =
               TypedFields.dump_data(%{name: 123, count: nil})
    end

    test "load_params fails on invalid stored value" do
      assert {:error, %CastError{field: :required_int, operation: :load}} =
               TypedFields.load_params(%{"required_int" => "not-a-number", "amount" => "1"})
    end
  end

  describe "Interruptus.Type.Decimal" do
    test "dumps normalized string and loads back to Decimal" do
      decimal = Decimal.new("10.5")
      assert {:ok, "10.5"} = DecimalType.dump(decimal)
      assert {:ok, ^decimal} = DecimalType.load("10.5")
    end
  end
end
