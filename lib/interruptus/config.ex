defmodule Interruptus.Config do
  @moduledoc """
  Runtime configuration for Interruptus.

  Configuration is built from application env (`config :interruptus, Interruptus, ...`)
  merged with overrides passed to `{Interruptus, repo: MyApp.Repo}` or
  `Interruptus.Config.new/1`.
  Stored in `:persistent_term` via `put/1` for fast access during execution.

  ## Struct fields

    * `:name` - config instance name atom (default `Interruptus`)
    * `:repo` - host Ecto repo module used for Interruptus persistence (required).
      May be the application Repo or a dedicated pool Repo pointing at the same database.
    * `:prefix` - PostgreSQL schema prefix (default `"public"`)
    * `:node_id` - cluster node identifier for leases. Defaults to
      `"#{Node.self()}/<boot-token>"` where the boot token is random per VM
      start, so non-distributed nodes (all named `nonode@nohost`) remain
      distinguishable.
    * `:lease_duration` - lease TTL in milliseconds (default `30_000`)
    * `:heartbeat_interval` - runner lease renewal interval in ms (default `10_000`)
    * `:recovery_interval` - reclaim scan interval in ms (default `5_000`);
      a small random jitter is added to each scan to avoid thundering herds
    * `:recovery_schedule` - whether `Interruptus.Recovery` scans periodically
      (default `true`; tests typically disable it and call
      `Interruptus.Recovery.recover_all/1` manually)

  ## Process names

  Each Interruptus instance runs its own supervision tree under the host
  application. Process names are derived from `:name` via `Module.concat/2`,
  e.g. the default instance uses `Interruptus.Registry`,
  `Interruptus.RunnerSupervisor`, `Interruptus.TaskSupervisor`, and
  `Interruptus.Recovery`.
  """

  @type t :: %__MODULE__{
          name: atom(),
          repo: module(),
          prefix: String.t(),
          node_id: String.t(),
          lease_duration: pos_integer(),
          heartbeat_interval: pos_integer(),
          recovery_interval: pos_integer(),
          recovery_schedule: boolean()
        }

  defstruct name: Interruptus,
            repo: nil,
            prefix: "public",
            node_id: nil,
            lease_duration: 30_000,
            heartbeat_interval: 10_000,
            recovery_interval: 5_000,
            recovery_schedule: true

  @doc """
  Builds configuration from application env and optional overrides.

  Merges `opts` over `Application.get_env(:interruptus, name, [])`. When
  `:node_id` is omitted, it defaults to the current node name plus a random
  per-boot token.

  ## Arguments

    * `opts` - keyword list of config fields and `:name`

  ## Returns

    * `%Interruptus.Config{}` struct

  ## Raises

    * `ArgumentError` - when `struct!/2` receives unknown or invalid keys
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

  Reads from `:persistent_term` when previously stored via `put/1`, otherwise
  builds a fresh config with `new/1`.

  ## Arguments

    * `name` - config name atom (default `Interruptus`)

  ## Returns

    * `%Interruptus.Config{}` struct
  """
  @spec fetch(atom()) :: t()
  def fetch(name \\ Interruptus) do
    case :persistent_term.get({__MODULE__, name}, nil) do
      nil -> new(name: name)
      config -> config
    end
  end

  @doc """
  Stores configuration in `:persistent_term` for fast access.

  Called during `Interruptus` child startup. Returns the same struct.

  ## Arguments

    * `config` - config struct to store

  ## Returns

    * The same `%Interruptus.Config{}` struct
  """
  @spec put(t()) :: t()
  def put(%__MODULE__{name: name} = config) do
    :persistent_term.put({__MODULE__, name}, config)
    config
  end

  @doc """
  Returns the registered name of the per-instance supervisor.
  """
  @spec supervisor_name(t() | atom()) :: atom()
  def supervisor_name(config), do: process_name(config, "Supervisor")

  @doc """
  Returns the registered name of the per-instance runner Registry.
  """
  @spec registry_name(t() | atom()) :: atom()
  def registry_name(config), do: process_name(config, "Registry")

  @doc """
  Returns the registered name of the per-instance runner DynamicSupervisor.
  """
  @spec runner_supervisor_name(t() | atom()) :: atom()
  def runner_supervisor_name(config), do: process_name(config, "RunnerSupervisor")

  @doc """
  Returns the registered name of the per-instance stage Task.Supervisor.
  """
  @spec task_supervisor_name(t() | atom()) :: atom()
  def task_supervisor_name(config), do: process_name(config, "TaskSupervisor")

  @doc """
  Returns the registered name of the per-instance Recovery process.
  """
  @spec recovery_name(t() | atom()) :: atom()
  def recovery_name(config), do: process_name(config, "Recovery")

  @spec process_name(t() | atom(), String.t()) :: atom()
  defp process_name(%__MODULE__{name: name}, suffix), do: process_name(name, suffix)

  defp process_name(name, suffix) when is_atom(name) do
    Module.concat(name, suffix)
  end

  @spec validate!(t()) :: t()
  defp validate!(%__MODULE__{node_id: nil} = config) do
    %{config | node_id: "#{Node.self()}/#{boot_token()}"}
  end

  defp validate!(config), do: config

  # A random token generated once per VM start. Ensures every BEAM instance
  # has a distinct lease identity even when nodes are not distributed (all
  # named nonode@nohost) or share a node name across restarts.
  @spec boot_token() :: String.t()
  defp boot_token do
    case :persistent_term.get({__MODULE__, :boot_token}, nil) do
      nil ->
        token =
          :crypto.strong_rand_bytes(5)
          |> Base.encode32(case: :lower, padding: false)

        :persistent_term.put({__MODULE__, :boot_token}, token)
        token

      token ->
        token
    end
  end
end
