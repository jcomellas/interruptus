defmodule Interruptus.Effect do
  @moduledoc """
  Idempotent effect markers for stage side effects on the shared database.

  Interruptus runs stages **outside** library transactions and re-executes
  segments at-least-once after crashes. Use these helpers to **claim** an
  effect (`:pending`), apply work, then mark it `:applied` so replay skips it.

  ## Important semantics

  * `once/4` inserts a `:pending` marker **before** running `fun`, then marks
    `:applied` on success. Concurrent callers serialize on the unique key.
  * Skip only when a marker is already `:applied`.
  * On suspend, halt, error, or crash of `fun`, the pending marker is deleted
    so a later retry can claim again.
  * A stale `:pending` marker (older than the Interruptus lease duration) may
    be reclaimed by a later `once/4`.
  *   Prefer `exists?/3` inside checkpoint `verify/1` functions (true only for
  `:applied`), and `once/4` in the matching stage:

      def verify_debit(command) do
        if Effect.exists?(command, Effect.key(["debit", command.params.ref])) do
          :done
        else
          :not_done
        end
      end

      def debit_account(command, params, _data) do
        Effect.once(command, Effect.key(["debit", params.ref]), fn cmd ->
          # ... apply side effect ...
          cmd
        end)
      end

  The runtime command carries `workflow_id` (set by `Interruptus.Runner`).
  In-memory `Workflow.new/1` / `run/1` have `workflow_id: nil` and cannot
  persist markers — use `Interruptus.Test.assign_workflow_id/2` in tests.

  ## Options

    * `:config` - Interruptus config name atom (default `Interruptus`)
    * `:metadata` - map stored on the marker (default `%{}`)
    * `:stale_after` - pending reclaim threshold in ms (default: config lease_duration)
  """

  import Ecto.Query

  require Logger

  alias Interruptus.Config
  alias Interruptus.Repo
  alias Interruptus.Schemas.Effect

  @doc """
  Builds a stable effect key from parts.

  Parts are joined with `":"`. Atoms and numbers are stringified.

  ## Examples

      iex> Interruptus.Effect.key(["debit", 42])
      "debit:42"

      iex> Interruptus.Effect.key([:credit, "abc"])
      "credit:abc"
  """
  @spec key([term()]) :: String.t()
  def key(parts) when is_list(parts) and parts != [] do
    parts
    |> Enum.map(&to_string/1)
    |> Enum.join(":")
  end

  @doc """
  Returns whether an **applied** effect marker already exists.

  Accepts a command with `workflow_id`, or a bare workflow UUID string.

  ## Arguments

    * `command_or_id` - workflow command or workflow UUID
    * `effect_key` - string key unique within the workflow
    * `opts` - optional `:config` name

  ## Returns

    * `true` when an `:applied` row exists
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
        where:
          e.workflow_id == ^workflow_id and e.effect_key == ^effect_key and e.status == :applied,
        select: 1,
        limit: 1
      )
    ) == 1
  end

  def exists?(_, _, _), do: false

  @doc """
  Inserts an effect marker for the command's workflow.

  Defaults to `:applied` status for explicit durable markers (e.g. from verify
  helpers). Prefer `once/4` for claim-before-apply stage work.

  ## Arguments

    * `command` - workflow command with `workflow_id`
    * `effect_key` - string key unique within the workflow
    * `metadata` - optional JSON-serializable map
    * `opts` - optional `:config` name; optional `:status` (`:pending` | `:applied`)

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
    status = Keyword.get(opts, :status, :applied)

    %Effect{}
    |> Effect.changeset(%{
      workflow_id: workflow_id,
      effect_key: effect_key,
      metadata: metadata,
      status: status
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
  Claims an effect key, runs `fun` once, and marks the effect applied.

  Algorithm:

  1. Try to insert a `:pending` marker.
  2. If an `:applied` marker exists, return `command` unchanged.
  3. If a fresh `:pending` marker exists, return `{:error, :effect_in_progress}`.
  4. If a stale `:pending` marker exists, take it over and continue.
  5. Run `fun`. On success map result, mark `:applied`. On suspend/halt/error,
     delete the pending marker and propagate the result (errors may include the
     mutated command as a 3-tuple).

  ## Arguments

    * `command` - workflow command with `workflow_id`
    * `effect_key` - string key unique within the workflow
    * `fun` - `(command -> command | {:suspend, ...} | {:error, ...})`
    * `opts` - `:config`, `:metadata`, `:stale_after`

  ## Returns

    * Updated command (or original when skipped)
    * Suspend tuples from `fun`
    * `{:error, :effect_in_progress}` when another claim is active
    * `{:error, reason}` or `{:error, reason, command}` from `fun` / marker failures
  """
  @spec once(struct(), String.t(), (struct() -> term()), keyword()) :: term()
  def once(command, effect_key, fun, opts \\ [])
      when is_binary(effect_key) and is_function(fun, 1) do
    case claim(command, effect_key, opts) do
      :skip ->
        command

      {:ok, _effect} ->
        run_claimed(command, effect_key, fun, opts)

      {:error, :missing_workflow_id} ->
        {:error, {:effect_marker_failed, :missing_workflow_id}, command}

      {:error, :effect_in_progress} ->
        {:error, :effect_in_progress, command}

      {:error, reason} ->
        {:error, {:effect_marker_failed, reason}, command}
    end
  end

  @spec claim(struct(), String.t(), keyword()) ::
          :skip | {:ok, Effect.t()} | {:error, term()}
  defp claim(%{workflow_id: workflow_id} = command, effect_key, opts)
       when is_binary(workflow_id) do
    config = config_from_opts(opts)
    metadata = Keyword.get(opts, :metadata, %{})
    stale_after = Keyword.get(opts, :stale_after, config.lease_duration)
    now = DateTime.utc_now()

    case put(command, effect_key, metadata, Keyword.put(opts, :status, :pending)) do
      {:ok, effect} ->
        {:ok, effect}

      {:error, :already_exists} ->
        resolve_existing(config, workflow_id, effect_key, stale_after, now)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp claim(_command, _effect_key, _opts), do: {:error, :missing_workflow_id}

  @spec resolve_existing(Config.t(), Ecto.UUID.t(), String.t(), pos_integer(), DateTime.t()) ::
          :skip | {:ok, Effect.t()} | {:error, term()}
  defp resolve_existing(config, workflow_id, effect_key, stale_after, now) do
    case get_effect(config, workflow_id, effect_key) do
      %Effect{status: :applied} ->
        :skip

      %Effect{status: :pending, updated_at: updated_at} = effect ->
        if stale_pending?(updated_at, now, stale_after) do
          case touch_pending(config, effect, now) do
            {:ok, updated} -> {:ok, updated}
            {:error, reason} -> {:error, reason}
          end
        else
          {:error, :effect_in_progress}
        end

      nil ->
        # Lost the race then row disappeared; ask caller to retry.
        {:error, :effect_in_progress}
    end
  end

  @spec run_claimed(struct(), String.t(), (struct() -> term()), keyword()) :: term()
  defp run_claimed(command, effect_key, fun, opts) do
    try do
      case fun.(command) do
        {:suspend, _reason, _metadata, _command} = suspended ->
          release_pending(command, effect_key, opts)
          suspended

        {:suspend, _reason, _metadata} = suspended ->
          release_pending(command, effect_key, opts)
          suspended

        %{halted: true} = halted ->
          release_pending(command, effect_key, opts)
          halted

        {:error, reason, %{} = failed_command} ->
          release_pending(command, effect_key, opts)
          {:error, reason, failed_command}

        {:error, reason} ->
          release_pending(command, effect_key, opts)
          {:error, reason, command}

        %{} = result ->
          case mark_applied(command, effect_key, opts) do
            :ok ->
              result

            {:error, reason} ->
              release_pending(command, effect_key, opts)

              Logger.warning(
                "interruptus effect marker apply failed effect_key=#{effect_key}: " <>
                  "#{inspect(reason)}"
              )

              {:error, {:effect_marker_failed, reason}, result}
          end

        other ->
          release_pending(command, effect_key, opts)
          {:error, {:invalid_effect_result, other}, command}
      end
    rescue
      exception ->
        release_pending(command, effect_key, opts)
        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        release_pending(command, effect_key, opts)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @spec mark_applied(struct(), String.t(), keyword()) :: :ok | {:error, term()}
  defp mark_applied(%{workflow_id: workflow_id}, effect_key, opts) when is_binary(workflow_id) do
    config = config_from_opts(opts)
    now = DateTime.utc_now()

    {count, _} =
      Repo.update_all(
        config,
        from(e in Effect,
          where:
            e.workflow_id == ^workflow_id and e.effect_key == ^effect_key and e.status == :pending
        ),
        set: [status: :applied, updated_at: now]
      )

    case count do
      1 -> :ok
      0 -> {:error, :stale_pending}
    end
  end

  @spec release_pending(struct(), String.t(), keyword()) :: :ok
  defp release_pending(%{workflow_id: workflow_id}, effect_key, opts) when is_binary(workflow_id) do
    config = config_from_opts(opts)

    _ =
      Repo.delete_all(
        config,
        from(e in Effect,
          where:
            e.workflow_id == ^workflow_id and e.effect_key == ^effect_key and e.status == :pending
        )
      )

    :ok
  end

  defp release_pending(_, _, _), do: :ok

  @spec get_effect(Config.t(), Ecto.UUID.t(), String.t()) :: Effect.t() | nil
  defp get_effect(config, workflow_id, effect_key) do
    Repo.one(
      config,
      from(e in Effect,
        where: e.workflow_id == ^workflow_id and e.effect_key == ^effect_key,
        limit: 1
      )
    )
  end

  @spec touch_pending(Config.t(), Effect.t(), DateTime.t()) ::
          {:ok, Effect.t()} | {:error, term()}
  defp touch_pending(config, %Effect{id: id, updated_at: previous} = effect, now) do
    {count, _} =
      Repo.update_all(
        config,
        from(e in Effect,
          where: e.id == ^id and e.status == :pending and e.updated_at == ^previous
        ),
        set: [updated_at: now]
      )

    case count do
      1 -> {:ok, %{effect | updated_at: now}}
      0 -> {:error, :effect_in_progress}
    end
  end

  @spec stale_pending?(DateTime.t() | nil, DateTime.t(), pos_integer()) :: boolean()
  defp stale_pending?(nil, _now, _stale_after), do: true

  defp stale_pending?(updated_at, now, stale_after) do
    DateTime.diff(now, updated_at, :millisecond) >= stale_after
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
