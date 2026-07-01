defmodule Interruptus.Config do
  @moduledoc """
  Runtime configuration for Interruptus.
  """

  @type t :: %__MODULE__{
          name: atom(),
          repo: module(),
          prefix: String.t(),
          node_id: String.t(),
          lease_duration: pos_integer(),
          heartbeat_interval: pos_integer(),
          recovery_interval: pos_integer()
        }

  defstruct name: Interruptus,
            repo: nil,
            prefix: "public",
            node_id: nil,
            lease_duration: 30_000,
            heartbeat_interval: 10_000,
            recovery_interval: 5_000

  @doc """
  Builds configuration from application env and optional overrides.
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    name = Keyword.get(opts, :name, Interruptus)
    app_opts = Application.get_env(:interruptus, name, [])

    merged =
      app_opts
      |> Keyword.merge(opts)
      |> Keyword.drop([:name])

    struct!(__MODULE__, Keyword.put(merged, :name, name))
    |> validate!()
  end

  @doc """
  Fetches configuration for the given Interruptus instance name.
  """
  @spec fetch(atom()) :: t()
  def fetch(name \\ Interruptus) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      nil -> new(name: name)
      config -> config
    end
  end

  @doc """
  Stores configuration in persistent term for fast access.
  """
  @spec put(t()) :: t()
  def put(%__MODULE__{name: name} = config) do
    :persistent_term.put({__MODULE__, name}, config)
    config
  end

  defp validate!(%__MODULE__{node_id: nil} = config) do
    node = Atom.to_string(Node.self())
    %{config | node_id: node}
  end

  defp validate!(config), do: config
end
