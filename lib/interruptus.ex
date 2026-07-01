defmodule Interruptus do
  @moduledoc """
  Durable Commandex-style workflow pipelines for Elixir.

  See `Interruptus.Workflow` for defining workflows and `DESIGN.md` for architecture.
  """

  alias Interruptus.Config
  alias Interruptus.RunnerSupervisor
  alias Interruptus.Schemas.WorkflowInstance
  alias Interruptus.Store

  @doc """
  Child spec for the host application supervisor.

      {Interruptus, repo: MyApp.Repo}
  """
  def child_spec(opts) do
    config = Config.new(opts) |> Config.put()

    %{
      id: config.name,
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @doc false
  def start_link(opts) do
    config = Config.new(opts) |> Config.put()
    Interruptus.Recovery.recover_all(config)
    :ignore
  end

  @doc """
  Starts a new workflow instance.

  ## Options

    * `:idempotency_key` - optional unique key per workflow type
    * `:config` - Interruptus config name (default `Interruptus`)

  ## Examples

      Interruptus.start(MyApp.TransferFunds, %{amount: 100})
  """
  @spec start(module(), map() | keyword(), keyword()) ::
          {:ok, WorkflowInstance.t()} | {:error, term()}
  def start(workflow_module, params, opts \\ []) do
    config = config_from_opts(opts)

    attrs = %{
      workflow_type: workflow_module |> Module.split() |> Enum.join("."),
      status: :pending,
      params: normalize_map(params),
      data: %{},
      current_stage_index: 0,
      pipeline_version: workflow_module.pipeline_version(),
      idempotency_key: Keyword.get(opts, :idempotency_key)
    }

    with {:ok, instance} <- Store.insert_workflow(config, attrs),
         {:ok, _pid} <- RunnerSupervisor.start_runner(config, workflow_module, instance.id) do
      :telemetry.execute(
        [:interruptus, :workflow, :started],
        %{},
        %{workflow_id: instance.id, workflow_type: instance.workflow_type}
      )

      {:ok, instance}
    end
  end

  @doc """
  Resumes a suspended or reclaimable workflow.
  """
  @spec resume(Ecto.UUID.t(), keyword()) :: {:ok, pid()} | {:error, term()}
  def resume(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    with %WorkflowInstance{} = instance <- Store.get(config, workflow_id),
         false <- WorkflowInstance.terminal?(instance),
         workflow_module <- module_from_type(instance.workflow_type),
         {:ok, pid} <- RunnerSupervisor.start_runner(config, workflow_module, workflow_id) do
      :telemetry.execute(
        [:interruptus, :workflow, :resumed],
        %{},
        %{workflow_id: workflow_id}
      )

      {:ok, pid}
    else
      nil -> {:error, :not_found}
      true -> {:error, :terminal}
      error -> error
    end
  end

  @doc """
  Cancels a non-terminal workflow.
  """
  @spec cancel(Ecto.UUID.t(), keyword()) :: {:ok, WorkflowInstance.t()} | {:error, term()}
  def cancel(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    with %WorkflowInstance{} = instance <- Store.get(config, workflow_id),
         false <- WorkflowInstance.terminal?(instance),
         {:ok, cancelled} <-
           Store.update_with_lock(config, instance, %{
             status: :cancelled,
             locked_by: nil,
             locked_until: nil
           }) do
      :telemetry.execute(
        [:interruptus, :workflow, :cancelled],
        %{},
        %{workflow_id: workflow_id}
      )

      {:ok, cancelled}
    else
      nil -> {:error, :not_found}
      true -> {:error, :terminal}
      error -> error
    end
  end

  @doc """
  Returns the current status of a workflow instance.
  """
  @spec status(Ecto.UUID.t(), keyword()) :: {:ok, WorkflowInstance.t()} | {:error, :not_found}
  def status(workflow_id, opts \\ []) do
    config = config_from_opts(opts)

    case Store.get(config, workflow_id) do
      nil -> {:error, :not_found}
      instance -> {:ok, instance}
    end
  end

  defp config_from_opts(opts) do
    opts
    |> Keyword.get(:config, Interruptus)
    |> Config.fetch()
  end

  defp module_from_type(type) do
    type |> String.split(".") |> Module.concat()
  end

  defp normalize_map(params) when is_list(params), do: params |> Map.new() |> normalize_map()

  defp normalize_map(params) when is_map(params) do
    Map.new(params, fn
      {k, v} when is_atom(k) -> {Atom.to_string(k), v}
      {k, v} -> {k, v}
    end)
  end
end
