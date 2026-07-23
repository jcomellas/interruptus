defmodule Interruptus.Workflow.Fields do
  @moduledoc false

  import Ecto.Changeset

  alias Interruptus.Workflow.CastError

  @doc """
  Casts user input into a params map using an embedded schema module.
  """
  @spec cast_params(module(), [atom()], map() | keyword()) ::
          {:ok, map()} | {:error, Ecto.Changeset.t()}
  def cast_params(schema_mod, required_fields, input) do
    attrs = normalize_input_keys(input)
    fields = schema_mod.__schema__(:fields)

    schema_mod
    |> struct()
    |> schema_mod.changeset(attrs)
    |> validate_required_param_fields(required_fields)
    |> case do
      %{valid?: true} = changeset ->
        {:ok, embedded_to_map(apply_changes(changeset), fields)}

      changeset ->
        {:error, changeset}
    end
  end

  @doc """
  Loads persisted JSON (string keys) into an atom-keyed map.
  """
  @spec load_fields(module(), map()) :: {:ok, map()} | {:error, CastError.t()}
  def load_fields(schema_mod, json_map) when is_map(json_map) do
    fields = schema_mod.__schema__(:fields)

    Enum.reduce_while(fields, {:ok, %{}}, fn field, {:ok, acc} ->
      type = schema_mod.__schema__(:type, field)
      key = Atom.to_string(field)

      # Absent keys are omitted so Map.merge(defaults, loaded) keeps declared
      # defaults. Explicit JSON null still loads as nil and overrides defaults.
      case fetch_json_value(json_map, key, field) do
        :absent ->
          {:cont, {:ok, acc}}

        {:ok, value} ->
          case Ecto.Type.load(type, value) do
            {:ok, loaded} ->
              {:cont, {:ok, Map.put(acc, field, loaded)}}

            :error ->
              {:halt,
               {:error,
                CastError.exception(
                  field: field,
                  value: value,
                  operation: :load,
                  reason: :invalid_type
                )}}
          end
      end
    end)
  end

  @spec fetch_json_value(map(), String.t(), atom()) :: :absent | {:ok, term()}
  defp fetch_json_value(json_map, key, field) do
    cond do
      Map.has_key?(json_map, key) -> {:ok, Map.get(json_map, key)}
      Map.has_key?(json_map, field) -> {:ok, Map.get(json_map, field)}
      true -> :absent
    end
  end

  @doc """
  Dumps an atom-keyed map to a JSON-safe map with string keys.
  """
  @spec dump_fields(module(), map(), keyword()) :: {:ok, map()} | {:error, CastError.t()}
  def dump_fields(schema_mod, atom_map, opts \\ []) when is_map(atom_map) do
    omit_nil? = Keyword.get(opts, :omit_nil, true)
    validate_dump? = Keyword.get(opts, :validate_dump, false)
    fields = schema_mod.__schema__(:fields)

    Enum.reduce_while(fields, {:ok, %{}}, fn field, {:ok, acc} ->
      value = Map.get(atom_map, field)

      if omit_nil? and is_nil(value) do
        {:cont, {:ok, acc}}
      else
        dump_field(schema_mod, field, value, validate_dump?)
        |> case do
          {:ok, dumped} ->
            {:cont, {:ok, Map.put(acc, Atom.to_string(field), dumped)}}

          {:error, %CastError{} = error} ->
            {:halt, {:error, error}}
        end
      end
    end)
  end

  @doc """
  Converts an embedded schema struct to an atom-keyed map for all declared fields.
  """
  @spec embedded_to_map(struct(), [atom()]) :: map()
  def embedded_to_map(struct, fields) do
    Map.new(fields, fn field -> {field, Map.get(struct, field)} end)
  end

  @doc """
  Normalizes input keys to atoms for changeset casting.

  String keys that do not correspond to an existing atom are **dropped**
  rather than converted with `String.to_atom/1`: declared workflow fields
  always have existing atoms, and minting atoms from caller input would allow
  atom-table exhaustion.
  """
  @spec normalize_input_keys(map() | keyword()) :: map()
  def normalize_input_keys(input) when is_list(input), do: normalize_input_keys(Map.new(input))

  def normalize_input_keys(input) when is_map(input) do
    input
    |> Enum.reduce(%{}, fn
      {k, v}, acc when is_binary(k) ->
        try do
          Map.put(acc, String.to_existing_atom(k), v)
        rescue
          ArgumentError -> acc
        end

      {k, v}, acc ->
        Map.put(acc, k, v)
    end)
  end

  @spec validate_required_param_fields(Ecto.Changeset.t(), [atom()]) :: Ecto.Changeset.t()
  defp validate_required_param_fields(changeset, required_fields) do
    validate_required(changeset, required_fields)
  end

  @spec dump_field(module(), atom(), term(), boolean()) :: {:ok, term()} | {:error, CastError.t()}
  defp dump_field(schema_mod, field, value, validate_dump?) do
    type = schema_mod.__schema__(:type, field)

    with {:ok, dumped} <- dump_type(type, value),
         :ok <- maybe_validate_dump(type, dumped, field, value, validate_dump?) do
      {:ok, dumped}
    else
      :error ->
        {:error, CastError.exception(field: field, value: value, operation: :dump, reason: :invalid_type)}

      {:error, %CastError{} = error} ->
        {:error, error}
    end
  end

  @spec dump_type(term(), term()) :: {:ok, term()} | :error
  defp dump_type(type, value) do
    case Ecto.Type.dump(type, value) do
      {:ok, dumped} -> {:ok, dumped}
      :error -> :error
    end
  end

  @spec maybe_validate_dump(term(), term(), atom(), term(), boolean()) ::
          :ok | {:error, CastError.t()}
  defp maybe_validate_dump(_type, _dumped, _field, _value, false), do: :ok

  defp maybe_validate_dump(type, dumped, field, value, true) do
    case Ecto.Type.cast(type, dumped) do
      {:ok, _} ->
        :ok

      :error ->
        {:error,
         CastError.exception(
           field: field,
           value: value,
           operation: :validate_dump,
           reason: :dump_not_castable
         )}
    end
  end
end
