defmodule Interruptus.Workflow do
  @moduledoc """
  Macro DSL for defining durable workflows.

  Workflows are Commandex-compatible: they define `param`, `data`, and `pipeline`
  stages, plus `checkpoint` segments with optional `verify` functions and policies.
  """

  @doc """
  Imports workflow definition macros into the calling module.
  """
  defmacro __using__(_opts) do
    quote do
      import Interruptus.Workflow
      alias Interruptus.Command

      Module.register_attribute(__MODULE__, :workflow_segments, accumulate: true)
      Module.register_attribute(__MODULE__, :workflow_params, accumulate: true)
      Module.register_attribute(__MODULE__, :workflow_data, accumulate: true)
      Module.register_attribute(__MODULE__, :workflow_restart_policy, accumulate: false)
      Module.register_attribute(__MODULE__, :workflow_rollback_policy, accumulate: false)
      Module.register_attribute(__MODULE__, :workflow_pipeline_version, accumulate: false)
      Module.register_attribute(__MODULE__, :workflow_current_segment, accumulate: false)

      @before_compile Interruptus.Workflow
    end
  end

  @doc """
  Defines a workflow module with params, data, pipelines, and policies.
  """
  defmacro workflow(do: block) do
    quote do
      @workflow_current_segment nil

      unquote(block)
    end
  end

  @doc "Defines a workflow parameter."
  defmacro param(name, opts \\ []) do
    quote do
      Interruptus.Workflow.__param__(__MODULE__, unquote(name), unquote(opts))
    end
  end

  @doc "Defines a workflow data field."
  defmacro data(name) do
    quote do
      Interruptus.Workflow.__data__(__MODULE__, unquote(name))
    end
  end

  @doc "Defines a pipeline stage outside a checkpoint segment."
  defmacro pipeline(name) do
    quote do
      Interruptus.Workflow.__pipeline__(__MODULE__, unquote(name))
    end
  end

  @doc """
  Defines a checkpoint segment with optional verify function and pipeline stages.
  """
  defmacro checkpoint(do: block) do
    quote do
      @workflow_current_segment %{verify: nil, pipelines: []}
      unquote(block)
      Interruptus.Workflow.__checkpoint_segment__(__MODULE__, @workflow_current_segment)
      @workflow_current_segment nil
    end
  end

  @doc "Defines a verify function for the current checkpoint segment."
  defmacro verify(name) do
    quote do
      @workflow_current_segment Map.put(@workflow_current_segment, :verify, unquote(name))
    end
  end

  @doc "Defines restart policy for the workflow."
  defmacro restart_policy(opts) do
    quote do
      @workflow_restart_policy unquote(opts)
    end
  end

  @doc "Defines rollback policy for the workflow."
  defmacro rollback_policy(opts) do
    quote do
      @workflow_rollback_policy unquote(opts)
    end
  end

  @doc "Sets pipeline version for migration tracking."
  defmacro pipeline_version(version) do
    quote do
      @workflow_pipeline_version unquote(version)
    end
  end

  defmacro __before_compile__(env) do
    segments = env.module |> Module.get_attribute(:workflow_segments) |> Enum.reverse()
    params = env.module |> Module.get_attribute(:workflow_params) |> Enum.reverse()
    data = env.module |> Module.get_attribute(:workflow_data) |> Enum.reverse()
    restart_policy = Module.get_attribute(env.module, :workflow_restart_policy) || []
    rollback_policy = Module.get_attribute(env.module, :workflow_rollback_policy) || []
    pipeline_version = Module.get_attribute(env.module, :workflow_pipeline_version) || 1

    param_defaults = for {name, default} <- params, into: %{}, do: {name, default}
    data_defaults = for name <- data, into: %{}, do: {name, nil}

    flattened = flatten_segments(segments)

    quote do
      @behaviour Interruptus.Workflow.Behaviour

      defstruct success: false,
                halted: false,
                errors: %{},
                params: unquote(Macro.escape(param_defaults)),
                data: unquote(Macro.escape(data_defaults)),
                pipelines: unquote(Macro.escape(flattened))

      @type t :: %__MODULE__{
              success: boolean(),
              halted: boolean(),
              errors: map(),
              params: map(),
              data: map(),
              pipelines: [Interruptus.Workflow.segment()]
            }

      @impl Interruptus.Workflow.Behaviour
      def workflow_type, do: __MODULE__

      @impl Interruptus.Workflow.Behaviour
      def segments, do: unquote(Macro.escape(segments))

      @impl Interruptus.Workflow.Behaviour
      def flattened_pipelines, do: unquote(Macro.escape(flattened))

      @impl Interruptus.Workflow.Behaviour
      def restart_policy, do: unquote(Macro.escape(normalize_restart_policy(restart_policy)))

      @impl Interruptus.Workflow.Behaviour
      def rollback_policy, do: unquote(Macro.escape(normalize_rollback_policy(rollback_policy)))

      @impl Interruptus.Workflow.Behaviour
      def pipeline_version, do: unquote(pipeline_version)

      @doc """
      Creates a new command struct from parameters.
      """
      @spec new(map() | Keyword.t()) :: t()
      def new(opts \\ []) do
        Interruptus.Command.parse_params(%__MODULE__{}, opts)
      end

      @doc """
      Runs all pipeline segments in memory without durability.
      """
      @spec run(map() | Keyword.t() | t()) :: t()
      def run(%__MODULE__{pipelines: pipelines} = command) do
        pipelines
        |> Enum.reduce_while(command, fn segment, acc ->
          case Interruptus.Engine.run_segment(__MODULE__, segment, acc) do
            {:ok, updated} -> {:cont, updated}
            {:halted, halted} -> {:halt, halted}
            {:suspend, _, _, _} = suspended -> {:halt, suspended}
            {:error, _} = err -> {:halt, err}
          end
        end)
        |> case do
          {:suspend, _, _, _} = suspended -> suspended
          {:error, _} = err -> err
          %{halted: true} = halted -> halted
          command -> Interruptus.Command.maybe_mark_successful(command)
        end
      end

      def run(params) do
        params |> new() |> run()
      end
    end
  end

  @doc false
  def __param__(mod, name, opts) do
    params = Module.get_attribute(mod, :workflow_params, [])

    if List.keyfind(params, name, 0) do
      raise ArgumentError, "param #{inspect(name)} is already defined"
    end

    default = Keyword.get(opts, :default)
    Module.put_attribute(mod, :workflow_params, {name, default})
  end

  @doc false
  def __data__(mod, name) do
    data = Module.get_attribute(mod, :workflow_data, [])

    if name in data do
      raise ArgumentError, "data #{inspect(name)} is already defined"
    end

    Module.put_attribute(mod, :workflow_data, name)
  end

  @doc false
  def __pipeline__(mod, name) do
    segment = Module.get_attribute(mod, :workflow_current_segment)

    if segment do
      updated = Map.update!(segment, :pipelines, &[name | &1])
      Module.put_attribute(mod, :workflow_current_segment, updated)
    else
      Module.put_attribute(mod, :workflow_segments, {:stage, name})
    end
  end

  @doc false
  def __checkpoint_segment__(mod, segment) do
    segment = %{segment | pipelines: Enum.reverse(segment.pipelines)}
    Module.put_attribute(mod, :workflow_segments, {:checkpoint, segment})
  end

  defp flatten_segments(segments) do
    Enum.flat_map(segments, fn
      {:stage, name} ->
        [%{type: :stage, name: name, verify: nil, pipelines: [name]}]

      {:checkpoint, segment} ->
        [%{type: :checkpoint, verify: segment.verify, pipelines: segment.pipelines}]
    end)
  end

  defp normalize_restart_policy([]),
    do: %{max_attempts: 3, backoff: :exponential, base_interval: 1_000, retryable_errors: :all}

  defp normalize_restart_policy(opts) do
    %{
      max_attempts: Keyword.get(opts, :max_attempts, 3),
      backoff: Keyword.get(opts, :backoff, :exponential),
      base_interval: Keyword.get(opts, :base_interval, 1_000),
      retryable_errors: Keyword.get(opts, :retryable_errors, :all)
    }
  end

  defp normalize_rollback_policy([]), do: %{compensate: []}

  defp normalize_rollback_policy(opts) do
    %{compensate: Keyword.get(opts, :compensate, [])}
  end
end

defmodule Interruptus.Workflow.Behaviour do
  @moduledoc """
  Behaviour implemented by workflow modules.
  """

  @callback workflow_type() :: module()
  @callback segments() :: [Interruptus.Workflow.segment()]
  @callback flattened_pipelines() :: [Interruptus.Workflow.segment()]
  @callback restart_policy() :: map()
  @callback rollback_policy() :: map()
  @callback pipeline_version() :: pos_integer()
end

defmodule Interruptus.Workflow.Segment do
  @moduledoc false
  @type t :: %{
          type: :stage | :checkpoint,
          name: atom() | nil,
          verify: atom() | nil,
          pipelines: [atom()]
        }
end
