defmodule Interruptus.WorkflowType do
  @moduledoc """
  Safe resolution of persisted `workflow_type` strings to workflow modules.

  Uses `Module.safe_concat/1` so database content cannot mint new atoms, and
  verifies the resolved module is loaded and implements the
  `Interruptus.Workflow.Behaviour` surface before returning it.
  """

  @doc """
  Resolves a workflow type string to its module.

  ## Arguments

    * `type` - dotted module string as stored in `workflow_type`
      (e.g. `"MyApp.TransferFunds"`)

  ## Returns

    * `{:ok, module}` - module exists and exposes `flattened_pipelines/0`
    * `{:error, :unknown_workflow_type}` - unknown atom, unloaded module, or
      module that is not an Interruptus workflow
  """
  @spec resolve(String.t()) :: {:ok, module()} | {:error, :unknown_workflow_type}
  def resolve(type) when is_binary(type) do
    module =
      type
      |> String.split(".")
      |> Module.safe_concat()

    if Code.ensure_loaded?(module) and function_exported?(module, :flattened_pipelines, 0) do
      {:ok, module}
    else
      {:error, :unknown_workflow_type}
    end
  rescue
    ArgumentError -> {:error, :unknown_workflow_type}
  end
end
