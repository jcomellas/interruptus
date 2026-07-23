defmodule Interruptus.Application do
  @moduledoc """
  OTP application callback for the `:interruptus` application.

  Intentionally starts **no** runtime processes. The Interruptus supervision
  tree (Registry, RunnerSupervisor, Task.Supervisor, Recovery) is per-instance
  and runs under the **host** application's supervisor via
  `{Interruptus, repo: MyApp.Repo}` — see `Interruptus.Supervisor`. Starting
  the tree in the host tree guarantees it boots after the host Repo and allows
  multiple named instances in one VM.
  """

  use Application

  # Application start callback. No global children; see moduledoc.
  @doc false
  @spec start(Application.start_type(), [term()]) :: Supervisor.on_start()
  @impl true
  def start(_type, _args) do
    Supervisor.start_link([], strategy: :one_for_one, name: Interruptus.ApplicationSupervisor)
  end
end
