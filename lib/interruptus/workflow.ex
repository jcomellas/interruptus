defmodule Interruptus.Workflow do
  @moduledoc """
  Macro DSL for defining durable workflows.

  Workflows define typed `param`, `data`, and `pipeline` stages, plus `checkpoint`
  segments with optional `verify` functions and policies.

  ## Example

      defmodule MyApp.TransferFunds do
        use Interruptus.Workflow

        workflow do
          param :from_account_id, :integer
          param :to_account_id, :integer
          param :amount, :decimal

          data :debit_ref, :string
          data :credit_ref, :string

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

  Params are cast at `Interruptus.start/3` and `new/1`. Data is validated when
  persisted to JSONB (dump-then-cast). Unset (`nil`) fields are omitted from
  stored JSON. Invalid load/dump fails the workflow.

  Start durable execution with `Interruptus.start/3` or run in-memory with `run/1`.

  ## Stage return values

    * Return the updated command struct for normal progress
    * `{:suspend, reason, metadata}` — voluntary suspension (pre-stage command)
    * `Interruptus.Command.suspend/3` — suspension that keeps command mutations
    * `Interruptus.Command.halt/1` — failure; triggers restart or rollback
    * `Interruptus.Command.halt(command, success: true)` — durable `:completed`
    * `{:error, reason}` — structured failure (e.g. effect marker insert failed)

  ## Verify functions

  Each checkpoint may define `verify :function_name`. The function receives the command
  and must return:

    * `:done` — external work already applied; skip segment stages
    * `:not_done` — re-run segment stages (at-least-once semantics)
    * `:failed` — unrecoverable; apply restart or rollback policy

  Verify functions must be idempotent and must not create duplicate side effects.

  For shared-database side effects, prefer `Interruptus.Effect.exists?/3` in
  verify and `Interruptus.Effect.once/4` in stages. Stages and Interruptus
  checkpoints are **not** one database transaction — expect at-least-once
  re-execution between checkpoints.

  ## Types

  See `Interruptus.Workflow.Segment` for the segment map type used in callbacks.
  """

  @type segment :: Interruptus.Workflow.Segment.t()

  @type raw_segment ::
          {:stage, atom()}
          | {:checkpoint, %{verify: atom() | nil, pipelines: [atom()], compensate: atom() | nil}}

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
      Module.register_attribute(__MODULE__, :workflow_stage_timeout, accumulate: false)
      Module.register_attribute(__MODULE__, :workflow_current_segment, accumulate: false)

      @before_compile Interruptus.Workflow
      @after_compile Interruptus.Workflow
    end
  end

  @doc """
  Opens a workflow definition block.

  All `param`, `data`, `pipeline`, `checkpoint`, and policy macros must appear
  inside this block.

  ## Examples

      workflow do
        param :amount, :decimal
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
  Declares a typed workflow parameter with an optional default.

  Parameters are provided at `Interruptus.start/3` or `new/1`, cast via
  `Ecto.Changeset`, and persisted in `params` on the workflow instance.

  ## Arguments

    * `name` - atom parameter name
    * `type` - `Ecto.Type` atom, `Ecto.Enum`, custom `Ecto.Type` module, or `:decimal`
    * `opts` - keyword list passed to `field/3` (e.g. `default:`, `values:` for enums)

  ## Options

    * `:default` - default value when not supplied at start time (also marks the
      field as optional for `validate_required` at start)

  ## Raises

    * `ArgumentError` - when the same param name is defined twice
  """
  defmacro param(name, type, opts \\ []) do
    quote do
      Interruptus.Workflow.__param__(__MODULE__, unquote(name), unquote(type), unquote(opts))
    end
  end

  @doc """
  Declares a typed workflow data field.

  Data fields are updated by pipeline stages via `Interruptus.Command.put_data/3`
  and persisted in `data` on the workflow instance between checkpoints. Values are
  validated on persist via dump-then-cast; unset (`nil`) fields are omitted from JSON.

  ## Arguments

    * `name` - atom data field name
    * `type` - `Ecto.Type` atom, `Ecto.Enum`, custom `Ecto.Type` module, or `:decimal`
    * `opts` - keyword list passed to `field/3`

  ## Raises

    * `ArgumentError` - when the same data name is defined twice
  """
  defmacro data(name, type, opts \\ []) do
    quote do
      Interruptus.Workflow.__data__(__MODULE__, unquote(name), unquote(type), unquote(opts))
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
  Declares a checkpoint segment with optional verify, compensation, and
  pipeline stages.

  Checkpoint boundaries define durability: the runner persists state after each
  checkpoint segment completes. Stages inside a checkpoint run at-least-once
  between checkpoints.

  Use `verify/1` inside the block to skip already-applied work.

  ## Options

    * `:compensate` - function atom invoked during rollback for this checkpoint
      when it has been **passed** (snapshot persisted) **or is in-flight**
      (current `current_stage_index`). Compensations run LIFO, followed by the
      workflow-level `rollback_policy/1` list. Compensations **must be
      idempotent**. When `compensate:` is set, `verify/1` is **required**.

  ## Examples

      checkpoint do
        verify :verify_payment_applied
        pipeline :capture_payment
      end

      checkpoint compensate: :refund_payment do
        verify :verify_payment_applied
        pipeline :capture_payment
      end
  """
  defmacro checkpoint(do: block) do
    build_checkpoint([], block)
  end

  defmacro checkpoint(opts, do: block) do
    build_checkpoint(opts, block)
  end

  @spec build_checkpoint(keyword(), Macro.t()) :: Macro.t()
  defp build_checkpoint(opts, block) do
    quote do
      @workflow_current_segment %{
        verify: nil,
        pipelines: [],
        compensate: Keyword.get(unquote(opts), :compensate)
      }
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
  Sets the pipeline version for intentional migration tracking.

  Stored on each workflow instance and **checked at claim time** together with
  the automatic `pipeline_fingerprint/0`. When the persisted version differs
  from the compiled module's version, the runner parks the workflow as
  `:suspended` with reason `"pipeline_version_mismatch"` instead of executing
  positional stage indexes against a different pipeline.

  Prefer bumping this when you intentionally change compensation or stage
  semantics for in-flight instances. Accidental layout edits are also caught by
  `pipeline_fingerprint/0` even if the version is unchanged.

  ## Arguments

    * `version` - positive integer version number
  """
  defmacro pipeline_version(version) do
    quote do
      @workflow_pipeline_version unquote(version)
    end
  end

  @doc """
  Sets the per-stage execution timeout in milliseconds.

  Each pipeline stage (and each compensation function) is aborted with a
  `:timeout` error when it exceeds this limit, which then flows through the
  workflow restart policy. Defaults to `:infinity`.

  ## Arguments

    * `timeout` - positive integer in milliseconds, or `:infinity`

  ## Examples

      workflow do
        stage_timeout 30_000
        pipeline :call_slow_api
      end
  """
  defmacro stage_timeout(timeout) do
    quote do
      @workflow_stage_timeout unquote(timeout)
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
    stage_timeout = Module.get_attribute(env.module, :workflow_stage_timeout) || :infinity

    param_defaults = param_defaults_map(params)
    data_defaults = data_defaults_map(data)
    required_params = required_param_fields(params)
    params_embed = generate_embedded_module(env.module, :Params, params)
    data_embed = generate_embedded_module(env.module, :Data, data)

    flattened = flatten_segments(segments)
    validate_compensate_requires_verify!(flattened, env)

    normalized_rollback = normalize_rollback_policy(rollback_policy)
    pipeline_fingerprint = compute_pipeline_fingerprint(flattened, normalized_rollback)

    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      unquote(params_embed)
      unquote(data_embed)

      @behaviour Interruptus.Workflow.Behaviour

      defstruct success: false,
                halted: false,
                errors: %{},
                params: unquote(Macro.escape(param_defaults)),
                data: unquote(Macro.escape(data_defaults)),
                pipelines: unquote(Macro.escape(flattened)),
                workflow_id: nil

      @type t :: %__MODULE__{
              success: boolean(),
              halted: boolean(),
              errors: map(),
              params: map(),
              data: map(),
              pipelines: [Interruptus.Workflow.segment()],
              workflow_id: Ecto.UUID.t() | nil
            }

      @doc """
      Returns the workflow module atom.

      Implements `Interruptus.Workflow.Behaviour`.
      """
      @spec workflow_type() :: module()
      @impl Interruptus.Workflow.Behaviour
      def workflow_type, do: __MODULE__

      @doc """
      Returns the raw segment list as defined in the workflow block.

      Each entry is `{:stage, name}` or `{:checkpoint, %{verify: ..., pipelines: ...}}`.
      """
      @spec segments() :: [Interruptus.Workflow.raw_segment()]
      @impl Interruptus.Workflow.Behaviour
      def segments, do: unquote(Macro.escape(segments))

      @doc """
      Returns flattened execution segments used by the engine and runner.

      Stages become `%{type: :stage, ...}` maps; checkpoints become
      `%{type: :checkpoint, verify: ..., pipelines: ...}`.
      """
      @spec flattened_pipelines() :: [Interruptus.Workflow.segment()]
      @impl Interruptus.Workflow.Behaviour
      def flattened_pipelines, do: unquote(Macro.escape(flattened))

      @doc """
      Returns the normalized restart policy map.

      See `Interruptus.Policy.Restart` for map shape.
      """
      @spec restart_policy() :: Interruptus.Policy.Restart.t()
      @impl Interruptus.Workflow.Behaviour
      def restart_policy, do: unquote(Macro.escape(normalize_restart_policy(restart_policy)))

      @doc """
      Returns the normalized rollback policy map with a `:compensate` function list.
      """
      @spec rollback_policy() :: Interruptus.Policy.Rollback.t()
      @impl Interruptus.Workflow.Behaviour
      def rollback_policy, do: unquote(Macro.escape(normalized_rollback))

      @doc """
      Returns the pipeline version integer stored on new instances.
      """
      @spec pipeline_version() :: pos_integer()
      @impl Interruptus.Workflow.Behaviour
      def pipeline_version, do: unquote(pipeline_version)

      @doc """
      Returns the structural fingerprint of the compiled pipeline layout.

      Compared at claim time against the value persisted on the instance row.
      """
      @spec pipeline_fingerprint() :: String.t()
      @impl Interruptus.Workflow.Behaviour
      def pipeline_fingerprint, do: unquote(pipeline_fingerprint)

      @doc """
      Returns the per-stage execution timeout (`:infinity` or milliseconds).
      """
      @spec stage_timeout() :: :infinity | pos_integer()
      @impl Interruptus.Workflow.Behaviour
      def stage_timeout, do: unquote(stage_timeout)

      @doc """
      Casts parameters and returns an atom-keyed params map.

      Required params (those without `default:`) are validated. See `cast_params!/1`
      for raising behaviour.
      """
      @spec cast_params(map() | Keyword.t()) :: {:ok, map()} | {:error, Ecto.Changeset.t()}
      @impl Interruptus.Workflow.Behaviour
      def cast_params(input) do
        unquote(cast_params_body(params, required_params))
      end

      @doc """
      Casts parameters or raises `Ecto.CastError`.
      """
      @spec cast_params!(map() | Keyword.t()) :: map()
      def cast_params!(input) do
        unquote(do_cast_params!(params))
      end

      @doc """
      Loads persisted params JSON into an atom-keyed map.
      """
      @spec load_params(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
      @impl Interruptus.Workflow.Behaviour
      def load_params(json_map) do
        unquote(load_params_body(params))
      end

      @doc """
      Loads persisted data JSON into an atom-keyed map.
      """
      @spec load_data(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
      @impl Interruptus.Workflow.Behaviour
      def load_data(json_map) do
        unquote(load_data_body(data))
      end

      @doc """
      Dumps params to a JSON-safe map with string keys. Omits `nil` fields.
      """
      @spec dump_params(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
      @impl Interruptus.Workflow.Behaviour
      def dump_params(atom_map) do
        unquote(dump_params_body(params))
      end

      @doc """
      Dumps data to a JSON-safe map with string keys. Omits `nil` fields and
      validates each dumped value via dump-then-cast.
      """
      @spec dump_data(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
      @impl Interruptus.Workflow.Behaviour
      def dump_data(atom_map) do
        unquote(dump_data_body(data))
      end

      @doc """
      Creates a new command struct from parameters.

      Casts supplied params via `cast_params!/1` and merges with declared defaults.

      ## Arguments

        * `opts` - keyword list or map of parameter values

      ## Returns

        * Command struct ready for `run/1` or runner execution
      """
      @spec new(map() | Keyword.t()) :: t()
      def new(opts \\ []) do
        params = cast_params!(opts)

        %__MODULE__{
          success: false,
          halted: false,
          errors: %{},
          params: Map.merge(unquote(Macro.escape(param_defaults)), params),
          data: unquote(Macro.escape(data_defaults)),
          pipelines: unquote(Macro.escape(flattened)),
          workflow_id: nil
        }
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
      @spec run(map() | Keyword.t() | t()) ::
              t()
              | {:suspend, term(), map(), t()}
              | {:error, term()}
              | {:error, term(), t()}
      def run(%__MODULE__{pipelines: pipelines} = command) do
        pipelines
        |> Enum.reduce_while(command, fn segment, acc ->
          case Interruptus.Engine.run_segment(__MODULE__, segment, acc) do
            {:ok, updated} -> {:cont, updated}
            {:halted, halted} -> {:halt, halted}
            {:suspend, _, _, _} = suspended -> {:halt, suspended}
            {:error, _, _} = err -> {:halt, err}
            {:error, _} = err -> {:halt, err}
          end
        end)
        |> case do
          {:suspend, _, _, _} = suspended ->
            suspended

          {:error, _, _} = err ->
            err

          {:error, _} = err ->
            err

          %{halted: true, success: true} = halted ->
            Interruptus.Command.maybe_mark_successful(%{halted | halted: false})

          %{halted: true} = halted ->
            halted

          command ->
            Interruptus.Command.maybe_mark_successful(command)
        end
      end

      def run(params) do
        params |> new() |> run()
      end
    end
  end

  # Compile-time helper for param/3. Raises ArgumentError on duplicate names.
  @doc false
  @spec __param__(module(), atom(), term(), keyword()) :: :ok
  def __param__(mod, name, type, opts) do
    params = Module.get_attribute(mod, :workflow_params, [])

    if List.keyfind(params, name, 0) do
      raise ArgumentError, "param #{inspect(name)} is already defined"
    end

    Module.put_attribute(mod, :workflow_params, {name, resolve_type(type), opts})
  end

  # Compile-time helper for data/3. Raises ArgumentError on duplicate names.
  @doc false
  @spec __data__(module(), atom(), term(), keyword()) :: :ok
  def __data__(mod, name, type, opts) do
    data = Module.get_attribute(mod, :workflow_data, [])

    if List.keyfind(data, name, 0) do
      raise ArgumentError, "data #{inspect(name)} is already defined"
    end

    Module.put_attribute(mod, :workflow_data, {name, resolve_type(type), opts})
  end

  @spec resolve_type(term()) :: term()
  defp resolve_type(:decimal), do: Interruptus.Type.Decimal
  defp resolve_type(type), do: type

  @spec param_defaults_map([{atom(), term(), keyword()}]) :: map()
  defp param_defaults_map(params) do
    Map.new(params, fn {name, _type, opts} -> {name, Keyword.get(opts, :default)} end)
  end

  @spec data_defaults_map([{atom(), term(), keyword()}]) :: map()
  defp data_defaults_map(data) do
    Map.new(data, fn {name, _type, opts} -> {name, Keyword.get(opts, :default)} end)
  end

  @spec required_param_fields([{atom(), term(), keyword()}]) :: [atom()]
  defp required_param_fields(params) do
    for {name, _type, opts} <- params, not Keyword.has_key?(opts, :default), do: name
  end

  @spec generate_embedded_module(module(), atom(), [{atom(), term(), keyword()}]) :: Macro.t()
  defp generate_embedded_module(_parent, _name, []), do: nil

  defp generate_embedded_module(parent, name, fields) do
    nested_mod = Module.concat([parent, name])
    field_names = Enum.map(fields, fn {field_name, _, _} -> field_name end)

    field_asts =
      Enum.map(fields, fn {field_name, type, opts} ->
        quote do
          field(unquote(field_name), unquote(Macro.escape(type)), unquote(Macro.escape(opts)))
        end
      end)

    quote do
      defmodule unquote(nested_mod) do
        use Ecto.Schema

        import Ecto.Changeset

        @primary_key false
        embedded_schema do
          (unquote_splicing(field_asts))
        end

        def changeset(struct, attrs) do
          cast(struct, attrs, unquote(field_names))
        end
      end
    end
  end

  @spec cast_params_body([{atom(), term(), keyword()}], [atom()]) :: Macro.t()
  defp cast_params_body([], _required), do: quote(do: {:ok, %{}})

  defp cast_params_body(_params, required) do
    quote do
      Interruptus.Workflow.Fields.cast_params(__MODULE__.Params, unquote(required), input)
    end
  end

  @spec do_cast_params!([{atom(), term(), keyword()}]) :: Macro.t()
  defp do_cast_params!([]), do: quote(do: %{})

  defp do_cast_params!(_params) do
    quote do
      case cast_params(input) do
        {:ok, params} -> params
        {:error, changeset} -> raise Ecto.CastError, changeset: changeset
      end
    end
  end

  @spec load_params_body([{atom(), term(), keyword()}]) :: Macro.t()
  defp load_params_body([]), do: quote(do: {:ok, %{}})

  defp load_params_body(_params) do
    quote do
      Interruptus.Workflow.Fields.load_fields(__MODULE__.Params, json_map)
    end
  end

  @spec load_data_body([{atom(), term(), keyword()}]) :: Macro.t()
  defp load_data_body([]), do: quote(do: {:ok, %{}})

  defp load_data_body(_data) do
    quote do
      Interruptus.Workflow.Fields.load_fields(__MODULE__.Data, json_map)
    end
  end

  @spec dump_params_body([{atom(), term(), keyword()}]) :: Macro.t()
  defp dump_params_body([]), do: quote(do: {:ok, %{}})

  defp dump_params_body(_params) do
    quote do
      Interruptus.Workflow.Fields.dump_fields(__MODULE__.Params, atom_map,
        omit_nil: true,
        validate_dump: false
      )
    end
  end

  @spec dump_data_body([{atom(), term(), keyword()}]) :: Macro.t()
  defp dump_data_body([]), do: quote(do: {:ok, %{}})

  defp dump_data_body(_data) do
    quote do
      Interruptus.Workflow.Fields.dump_fields(__MODULE__.Data, atom_map,
        omit_nil: true,
        validate_dump: true
      )
    end
  end

  # Compile-time helper for pipeline/1.
  @doc false
  @spec __pipeline__(module(), atom()) :: :ok
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
  @spec __checkpoint_segment__(module(), map()) :: :ok
  def __checkpoint_segment__(mod, segment) do
    segment = %{segment | pipelines: Enum.reverse(segment.pipelines)}
    Module.put_attribute(mod, :workflow_segments, {:checkpoint, segment})
  end

  @spec flatten_segments([raw_segment()]) :: [segment()]
  defp flatten_segments(segments) do
    Enum.flat_map(segments, fn
      {:stage, name} ->
        [%{type: :stage, name: name, verify: nil, pipelines: [name], compensate: nil}]

      {:checkpoint, segment} ->
        [
          %{
            type: :checkpoint,
            name: nil,
            verify: segment.verify,
            pipelines: segment.pipelines,
            compensate: Map.get(segment, :compensate)
          }
        ]
    end)
  end

  @spec validate_compensate_requires_verify!([segment()], Macro.Env.t()) :: :ok
  defp validate_compensate_requires_verify!(flattened, env) do
    Enum.each(flattened, fn
      %{type: :checkpoint, compensate: compensate, verify: nil} when not is_nil(compensate) ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "checkpoint with compensate: #{inspect(compensate)} requires verify/1 " <>
              "(compensations are tentative for in-flight segments and must reconcile via verify)"

      _ ->
        :ok
    end)
  end

  @spec compute_pipeline_fingerprint([segment()], Interruptus.Policy.Rollback.t()) :: String.t()
  defp compute_pipeline_fingerprint(flattened, rollback_policy) do
    canonical = {
      Enum.map(flattened, fn seg ->
        {seg.type, seg.name, seg.verify, seg.pipelines, seg.compensate}
      end),
      rollback_policy.compensate
    }

    :crypto.hash(:sha256, :erlang.term_to_binary(canonical))
    |> Base.encode16(case: :lower)
  end

  # Validates stage/verify/compensate callbacks exist after the module is compiled.
  @doc false
  @spec __after_compile__(Macro.Env.t(), binary()) :: :ok
  def __after_compile__(env, _bytecode) do
    mod = env.module
    flattened = mod.flattened_pipelines()
    rollback = mod.rollback_policy()

    for %{pipelines: pipelines} <- flattened, name <- pipelines do
      unless function_exported?(mod, name, 1) or function_exported?(mod, name, 3) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "#{inspect(mod)} is missing stage function #{name}/1 or #{name}/3"
      end
    end

    for %{verify: verify} <- flattened, is_atom(verify) and not is_nil(verify) do
      unless function_exported?(mod, verify, 1) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "#{inspect(mod)} is missing verify function #{verify}/1"
      end
    end

    for %{compensate: compensate} <- flattened, is_atom(compensate) and not is_nil(compensate) do
      unless function_exported?(mod, compensate, 1) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "#{inspect(mod)} is missing compensate function #{compensate}/1"
      end
    end

    for name <- rollback.compensate do
      unless function_exported?(mod, name, 1) do
        raise CompileError,
          file: env.file,
          line: env.line,
          description: "#{inspect(mod)} is missing rollback compensate function #{name}/1"
      end
    end

    :ok
  end

  @spec normalize_restart_policy(keyword() | []) :: Interruptus.Policy.Restart.t()
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

  @spec normalize_rollback_policy(keyword() | []) :: Interruptus.Policy.Rollback.t()
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

  ### `pipeline_fingerprint/0`

  Returns the structural fingerprint of the compiled pipeline (SHA-256 hex).

  ### `stage_timeout/0`

  Returns the per-stage execution timeout (`:infinity` or milliseconds).
  """

  @callback workflow_type() :: module()
  @callback segments() :: [Interruptus.Workflow.raw_segment()]
  @callback flattened_pipelines() :: [Interruptus.Workflow.segment()]
  @callback restart_policy() :: Interruptus.Policy.Restart.t()
  @callback rollback_policy() :: Interruptus.Policy.Rollback.t()
  @callback pipeline_version() :: pos_integer()
  @callback pipeline_fingerprint() :: String.t()
  @callback stage_timeout() :: :infinity | pos_integer()
  @callback cast_params(map() | keyword()) :: {:ok, map()} | {:error, Ecto.Changeset.t()}
  @callback load_params(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
  @callback load_data(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
  @callback dump_params(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
  @callback dump_data(map()) :: {:ok, map()} | {:error, Interruptus.Workflow.CastError.t()}
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
    * `:compensate` - compensation function atom for checkpoints, otherwise `nil`
  """

  @type t :: %{
          type: :stage | :checkpoint,
          name: atom() | nil,
          verify: atom() | nil,
          pipelines: [atom()],
          compensate: atom() | nil
        }
end
