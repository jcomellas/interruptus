defmodule Interruptus.Workflow do
  @moduledoc """
  Macro DSL for defining durable workflows.

  Workflows are Commandex-compatible: they define `param`, `data`, and `pipeline`
  stages, plus `checkpoint` segments with optional `verify` functions and policies.

  ## Example

      defmodule MyApp.TransferFunds do
        use Interruptus.Workflow

        workflow do
          param :from_account_id
          param :to_account_id
          param :amount

          data :debit_ref
          data :credit_ref

          pipeline :validate_accounts

          checkpoint do
            verify :verify_debit_applied
            pipeline :debit_account
          end

          checkpoint do
            verify :verify_credit_applied
            pipeline :credit_account
          end

          pipeline :send_receipt

          restart_policy max_attempts: 5, backoff: :exponential
          rollback_policy compensate: [:reverse_debit, :reverse_credit]
        end

        def validate_accounts(command, params, data), do: command
        def debit_account(command, params, data), do: command
        def verify_debit_applied(command), do: :not_done
        # ...
      end

  Start durable execution with `Interruptus.start/3` or run in-memory with `run/1`.

  ## Stage return values

    * Return the updated command struct for normal progress
    * `{:suspend, reason, metadata}` — voluntary suspension until `Interruptus.resume/2`
    * `Interruptus.Command.halt/2` — stop forward progress; triggers restart or rollback

  ## Verify functions

  Each checkpoint may define `verify :function_name`. The function receives the command
  and must return:

    * `:done` — external work already applied; skip segment stages
    * `:not_done` — re-run segment stages (at-least-once semantics)
    * `:failed` — unrecoverable; apply restart or rollback policy

  Verify functions must be idempotent and must not create duplicate side effects.

  ## Types

  See `Interruptus.Workflow.Segment` for the segment map type used in callbacks.
  """

  @type segment :: Interruptus.Workflow.Segment.t()

  @doc """
  Imports workflow definition macros into the calling module.

  Registers compile-time attributes and generates a command struct, behaviour
  callbacks, `new/1`, and `run/1` via `@before_compile`.

  ## Examples

      use Interruptus.Workflow
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
  Opens a workflow definition block.

  All `param`, `data`, `pipeline`, `checkpoint`, and policy macros must appear
  inside this block.

  ## Examples

      workflow do
        param :amount
        pipeline :charge
      end
  """
  defmacro workflow(do: block) do
    quote do
      @workflow_current_segment nil

      unquote(block)
    end
  end

  @doc """
  Declares a workflow parameter with an optional default.

  Parameters are provided at `Interruptus.start/3` or `new/1` and persisted in
  `params` on the workflow instance. Must be JSON-serializable.

  ## Arguments

    * `name` - atom parameter name
    * `opts` - keyword list

  ## Options

    * `:default` - default value when not supplied at start time

  ## Raises

    * `ArgumentError` - when the same param name is defined twice
  """
  defmacro param(name, opts \\ []) do
    quote do
      Interruptus.Workflow.__param__(__MODULE__, unquote(name), unquote(opts))
    end
  end

  @doc """
  Declares a workflow data field.

  Data fields are updated by pipeline stages via `Interruptus.Command.put_data/3`
  and persisted in `data` on the workflow instance between checkpoints.

  ## Arguments

    * `name` - atom data field name

  ## Raises

    * `ArgumentError` - when the same data name is defined twice
  """
  defmacro data(name) do
    quote do
      Interruptus.Workflow.__data__(__MODULE__, unquote(name))
    end
  end

  @doc """
  Declares a pipeline stage outside a checkpoint segment.

  Stages are plain functions `name(command, params, data)` on the workflow module.
  Outside a checkpoint, a stage is its own segment with no verify step.

  ## Arguments

    * `name` - atom matching a function on the workflow module
  """
  defmacro pipeline(name) do
    quote do
      Interruptus.Workflow.__pipeline__(__MODULE__, unquote(name))
    end
  end

  @doc """
  Declares a checkpoint segment with optional verify and pipeline stages.

  Checkpoint boundaries define durability: the runner persists state after each
  checkpoint segment completes. Stages inside a checkpoint run at-least-once
  between checkpoints.

  Use `verify/1` inside the block to skip already-applied work.

  ## Examples

      checkpoint do
        verify :verify_payment_applied
        pipeline :capture_payment
      end
  """
  defmacro checkpoint(do: block) do
    quote do
      @workflow_current_segment %{verify: nil, pipelines: []}
      unquote(block)
      Interruptus.Workflow.__checkpoint_segment__(__MODULE__, @workflow_current_segment)
      @workflow_current_segment nil
    end
  end

  @doc """
  Sets the verify function for the current checkpoint segment.

  The named function must accept the command struct and return `:done`,
  `:not_done`, or `:failed`.

  ## Arguments

    * `name` - atom matching `name/1` on the workflow module
  """
  defmacro verify(name) do
    quote do
      @workflow_current_segment Map.put(@workflow_current_segment, :verify, unquote(name))
    end
  end

  @doc """
  Declares restart policy for stage and verify failures.

  ## Options

    * `:max_attempts` - maximum retries before rollback (default `3`)
    * `:backoff` - `:constant` or `:exponential` (default `:exponential`)
    * `:base_interval` - base delay in ms (default `1_000`)
    * `:retryable_errors` - `:all` or a list of error terms (default `:all`)

  ## Examples

      restart_policy max_attempts: 5, backoff: :exponential, base_interval: 2_000
  """
  defmacro restart_policy(opts) do
    quote do
      @workflow_restart_policy unquote(opts)
    end
  end

  @doc """
  Declares rollback policy for terminal failure after retries are exhausted.

  ## Options

    * `:compensate` - list of function atoms invoked LIFO on the workflow module

  ## Examples

      rollback_policy compensate: [:reverse_debit, :reverse_credit]
  """
  defmacro rollback_policy(opts) do
    quote do
      @workflow_rollback_policy unquote(opts)
    end
  end

  @doc """
  Sets the pipeline version for migration tracking.

  Stored on each workflow instance so future pipeline changes can be detected.

  ## Arguments

    * `version` - positive integer version number
  """
  defmacro pipeline_version(version) do
    quote do
      @workflow_pipeline_version unquote(version)
    end
  end

  # Macro expansion emits the full workflow module; high complexity is expected here.
  # credo:disable-for-lines:136 Credo.Check.Refactor.CyclomaticComplexity
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

      @doc """
      Returns the workflow module atom.

      Implements `Interruptus.Workflow.Behaviour`.
      """
      @impl Interruptus.Workflow.Behaviour
      def workflow_type, do: __MODULE__

      @doc """
      Returns the raw segment list as defined in the workflow block.

      Each entry is `{:stage, name}` or `{:checkpoint, %{verify: ..., pipelines: ...}}`.
      """
      @impl Interruptus.Workflow.Behaviour
      def segments, do: unquote(Macro.escape(segments))

      @doc """
      Returns flattened execution segments used by the engine and runner.

      Stages become `%{type: :stage, ...}` maps; checkpoints become
      `%{type: :checkpoint, verify: ..., pipelines: ...}`.
      """
      @impl Interruptus.Workflow.Behaviour
      def flattened_pipelines, do: unquote(Macro.escape(flattened))

      @doc """
      Returns the normalized restart policy map.

      See `Interruptus.Policy.Restart` for map shape.
      """
      @impl Interruptus.Workflow.Behaviour
      def restart_policy, do: unquote(Macro.escape(normalize_restart_policy(restart_policy)))

      @doc """
      Returns the normalized rollback policy map with a `:compensate` function list.
      """
      @impl Interruptus.Workflow.Behaviour
      def rollback_policy, do: unquote(Macro.escape(normalize_rollback_policy(rollback_policy)))

      @doc """
      Returns the pipeline version integer stored on new instances.
      """
      @impl Interruptus.Workflow.Behaviour
      def pipeline_version, do: unquote(pipeline_version)

      @doc """
      Creates a new command struct from parameters.

      Merges supplied params with defaults declared via `param/2`.

      ## Arguments

        * `opts` - keyword list or map of parameter values

      ## Returns

        * Command struct ready for `run/1` or runner execution
      """
      @spec new(map() | Keyword.t()) :: t()
      def new(opts \\ []) do
        Interruptus.Command.parse_params(%__MODULE__{}, opts)
      end

      @doc """
      Runs all pipeline segments in memory without durability.

      Uses `Interruptus.Engine` internally. Does not write to the database.
      Useful for unit tests and dry runs.

      ## Arguments

        * `command` - command struct, or params to pass to `new/1`

      ## Returns

        * Successful command struct with `success: true`
        * Halted command struct
        * `{:suspend, reason, metadata, command}` tuple
        * `{:error, term()}` on failure
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

  # Compile-time helper for param/2. Raises ArgumentError on duplicate names.
  @doc false
  def __param__(mod, name, opts) do
    params = Module.get_attribute(mod, :workflow_params, [])

    if List.keyfind(params, name, 0) do
      raise ArgumentError, "param #{inspect(name)} is already defined"
    end

    default = Keyword.get(opts, :default)
    Module.put_attribute(mod, :workflow_params, {name, default})
  end

  # Compile-time helper for data/1. Raises ArgumentError on duplicate names.
  @doc false
  def __data__(mod, name) do
    data = Module.get_attribute(mod, :workflow_data, [])

    if name in data do
      raise ArgumentError, "data #{inspect(name)} is already defined"
    end

    Module.put_attribute(mod, :workflow_data, name)
  end

  # Compile-time helper for pipeline/1.
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

  # Compile-time helper for checkpoint/1. Finalizes the current checkpoint segment.
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
  Behaviour implemented by modules defined with `Interruptus.Workflow`.

  Callbacks are generated automatically by the `use Interruptus.Workflow` macro.

  ## Callbacks

  ### `workflow_type/0`

  Returns the defining module atom (`__MODULE__`).

  ### `segments/0`

  Returns the compile-time segment list before flattening.

  ### `flattened_pipelines/0`

  Returns the execution-order segment maps consumed by `Interruptus.Engine`.

  ### `restart_policy/0`

  Returns a map with `:max_attempts`, `:backoff`, `:base_interval`, and
  `:retryable_errors`. See `Interruptus.Policy.Restart`.

  ### `rollback_policy/0`

  Returns a map with `:compensate` — a list of compensation function atoms.

  ### `pipeline_version/0`

  Returns the positive integer pipeline version for instance rows.
  """

  @callback workflow_type() :: module()
  @callback segments() :: [Interruptus.Workflow.segment()]
  @callback flattened_pipelines() :: [Interruptus.Workflow.segment()]
  @callback restart_policy() :: map()
  @callback rollback_policy() :: map()
  @callback pipeline_version() :: pos_integer()
end

defmodule Interruptus.Workflow.Segment do
  @moduledoc """
  Type definition for flattened workflow execution segments.

  Produced by `flattened_pipelines/0` on workflow modules. Consumed by
  `Interruptus.Engine.run_segment/4`.

  ## Fields

    * `:type` - `:stage` for a single pipeline, `:checkpoint` for a durable segment
    * `:name` - stage name for `:stage` segments, otherwise `nil`
    * `:verify` - verify function atom for checkpoints, otherwise `nil`
    * `:pipelines` - ordered list of pipeline function atoms to run
  """

  @type t :: %{
          type: :stage | :checkpoint,
          name: atom() | nil,
          verify: atom() | nil,
          pipelines: [atom()]
        }
end
