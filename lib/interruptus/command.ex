defmodule Interruptus.Command do
  @moduledoc """
  Commandex-compatible helpers for workflow command structs.
  """

  @typedoc "Workflow command struct."
  @type t :: struct()

  @doc """
  Sets a data field with the given value.
  """
  @spec put_data(t(), atom(), any()) :: t()
  def put_data(%{data: data} = command, key, val) do
    %{command | data: Map.put(data, key, val)}
  end

  @doc """
  Sets an error for the given key.
  """
  @spec put_error(t(), any(), any()) :: t()
  def put_error(%{errors: errors} = command, key, val) do
    %{command | errors: Map.put(errors, key, val)}
  end

  @doc """
  Halts pipeline execution.
  """
  @spec halt(t(), keyword()) :: t()
  def halt(command, opts \\ []) do
    success = Keyword.get(opts, :success, false)
    %{command | halted: true, success: success}
  end

  @doc """
  Marks command as successful if not halted.
  """
  @spec maybe_mark_successful(t()) :: t()
  def maybe_mark_successful(%{halted: false} = command), do: %{command | success: true}
  def maybe_mark_successful(command), do: command

  @doc """
  Parses params into the command struct.
  """
  @spec parse_params(t(), map() | Keyword.t()) :: t()
  def parse_params(%{params: defaults} = struct, params) when is_list(params) do
    params =
      for {key, _} <- defaults, into: %{}, do: {key, Keyword.get(params, key, defaults[key])}

    %{struct | params: params}
  end

  def parse_params(%{params: defaults} = struct, %{} = params) do
    params =
      for {key, _} <- defaults, into: %{}, do: {key, get_param(params, key, defaults[key])}

    %{struct | params: params}
  end

  @doc """
  Applies a pipeline function to the command.
  """
  @spec apply_fun(t(), any()) :: t() | {:suspend, term(), map()}
  def apply_fun(%mod{params: params, data: data} = command, name) when is_atom(name) do
    apply(mod, name, [command, params, data])
  end

  def apply_fun(command, fun) when is_function(fun, 1), do: fun.(command)

  def apply_fun(%{params: params, data: data} = command, fun) when is_function(fun, 3) do
    fun.(command, params, data)
  end

  def apply_fun(%{params: params, data: data} = command, {m, f}) do
    apply(m, f, [command, params, data])
  end

  def apply_fun(%{params: params, data: data} = command, {m, f, a}) do
    apply(m, f, [command, params, data] ++ a)
  end

  defp get_param(params, key, default) do
    case Map.get(params, key) do
      nil -> Map.get(params, to_string(key), default)
      val -> val
    end
  end
end
