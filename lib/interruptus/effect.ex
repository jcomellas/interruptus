defmodule Interruptus.Effect do
  @moduledoc """
  Idempotent effect markers for stage side effects on the shared database.

  Interruptus runs stages **outside** library transactions and re-executes
  segments at-least-once after crashes. Use these helpers to record that an
  effect already ran, and to skip it on replay.

  ## Important semantics

  * Markers make **successful completion** skippable on replay.
  * A crash after the side effect but before `put/3` still re-runs the
    function — keep `fun` bodies idempotent or rely on domain unique
    constraints.
  * If `put/3` fails for a reason other than `:already_exists`, `once/4`
    returns `{:error, {:effect_marker_failed, reason}}` so the stage fails
    through the restart policy instead of silently succeeding.
  * Prefer `exists?/3` inside checkpoint `verify/1` functions.

  The runtime command carries `workflow_id` (set by `Interruptus.Runner`).
  In-memory `Workflow.new/1` / `run/1` have `workflow_id: nil` and cannot
  persist markers.

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)
  """

  import Ecto.Query

  require Logger

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.Effect

  @doc """
  Returns whether an effect marker already exists.

  Accepts a command with `workflow_id`, or a bare workflow UUID string.

  ## Arguments

    * `command_or_id` - workflow command or workflow UUID
    * `effect_key` - string key unique within the workflow
    * `opts` - optional `:config` name

  ## Returns

    * `true` when a row exists
    * `false` otherwise
  """
  @spec exists?(struct() | Ecto.UUID.t(), String.t(), keyword()) :: boolean()
  def exists?(command_or_id, effect_key, opts \\ [])

  def exists?(%{workflow_id: workflow_id}, effect_key, opts)
      when is_binary(workflow_id) and is_binary(effect_key) do
    exists?(workflow_id, effect_key, opts)
  end

  def exists?(workflow_id, effect_key, opts)
      when is_binary(workflow_id) and is_binary(effect_key) do
    config = config_from_opts(opts)

    Repo.one(
      config,
      from(e in Effect,
        where: e.workflow_id == ^workflow_id and e.effect_key == ^effect_key,
        select: 1,
        limit: 1
      )
    ) == 1
  end

  def exists?(_, _, _), do: false

  @doc """
  Inserts an effect marker for the command's workflow.

  ## Arguments

    * `command` - workflow command with `workflow_id`
    * `effect_key` - string key unique within the workflow
    * `metadata` - optional JSON-serializable map
    * `opts` - optional `:config` name

  ## Returns

    * `{:ok, %Effect{}}` - marker inserted
    * `{:error, :already_exists}` - unique key already present
    * `{:error, :missing_workflow_id}` - command has no `workflow_id`
    * `{:error, %Ecto.Changeset{}}` - other validation failures
  """
  @spec put(struct(), String.t(), map(), keyword()) ::
          {:ok, Effect.t()}
          | {:error, :already_exists | :missing_workflow_id | Ecto.Changeset.t()}
  def put(command, effect_key, metadata \\ %{}, opts \\ [])

  def put(%{workflow_id: workflow_id}, effect_key, metadata, opts)
      when is_binary(workflow_id) and is_binary(effect_key) and is_map(metadata) do
    config = config_from_opts(opts)

    %Effect{}
    |> Effect.changeset(%{
      workflow_id: workflow_id,
      effect_key: effect_key,
      metadata: metadata
    })
    |> then(&Repo.insert(config, &1))
    |> case do
      {:ok, effect} ->
        {:ok, effect}

      {:error, %Ecto.Changeset{errors: errors} = changeset} ->
        if unique_conflict?(errors) do
          {:error, :already_exists}
        else
          {:error, changeset}
        end
    end
  end

  def put(_command, _effect_key, _metadata, _opts), do: {:error, :missing_workflow_id}

  @doc """
  Runs `fun` once per `(workflow_id, effect_key)`, recording a marker on success.

  If a marker already exists, returns `command` unchanged without calling `fun`.
  On a successful non-suspend, non-halted command result, inserts the marker
  (unique races after success are treated as ok). No marker is written when:

  * `fun` returns a suspend tuple,
  * `fun` returns a **halted** command (`halted: true`) — the stage signalled
    failure, so the effect must not be considered applied on replay,
  * `fun` raises (the exception propagates).

  A marker insert failure other than a duplicate is logged and the result is
  still returned; the effect will re-run on replay (at-least-once).

  Note: markers protect against **sequential** replay after crashes and
  retries. They do not serialize two runners executing concurrently during a
  lease-expiry window — both may pass `exists?/3` before either inserts. Use
  domain unique constraints for hard once-only guarantees.

  ## Arguments

    * `command` - workflow command with `workflow_id`
    * `effect_key` - string key unique within the workflow
    * `fun` - `(command -> command | {:suspend, reason, metadata})`
    * `opts` - optional `:config` name; optional `:metadata` map stored on put

  ## Returns

    * Updated command (or original when skipped)
    * `{:suspend, reason, metadata}` or `{:suspend, reason, metadata, command}` when `fun` suspends
    * `{:error, {:effect_marker_failed, reason}}` when the marker cannot be persisted
      after a successful `fun` (restart policy applies; do not treat as success)
  """
  @spec once(struct(), String.t(), (struct() -> term()), keyword()) :: term()
  def once(command, effect_key, fun, opts \\ [])
      when is_binary(effect_key) and is_function(fun, 1) do
    if exists?(command, effect_key, opts) do
      command
    else
      case fun.(command) do
        {:suspend, _reason, _metadata, _command} = suspended ->
          suspended

        {:suspend, _reason, _metadata} = suspended ->
          suspended

        %{halted: true} = halted ->
          halted

        %{} = result ->
          metadata = Keyword.get(opts, :metadata, %{})

          case put(command, effect_key, metadata, opts) do
            {:ok, _} ->
              result

            {:error, :already_exists} ->
              result

            {:error, reason} ->
              Logger.warning(
                "interruptus effect marker insert failed effect_key=#{effect_key}: " <>
                  "#{inspect(reason)}"
              )

              {:error, {:effect_marker_failed, reason}}
          end
      end
    end
  end

  @spec config_from_opts(keyword()) :: Config.t()
  defp config_from_opts(opts) do
    opts
    |> Keyword.get(:config, Interruptus)
    |> Config.fetch()
  end

  @spec unique_conflict?([tuple()]) :: boolean()
  defp unique_conflict?(errors) do
    Enum.any?(errors, fn
      {:effect_key, {_, [constraint: :unique, constraint_name: _]}} ->
        true

      {_field, {_, [constraint: :unique, constraint_name: name]}} ->
        name in [:interruptus_effects_workflow_key_idx, "interruptus_effects_workflow_key_idx"]

      _ ->
        false
    end)
  end
end
