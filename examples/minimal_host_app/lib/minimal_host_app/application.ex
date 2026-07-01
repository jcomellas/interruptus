defmodule MinimalHostApp.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      MinimalHostApp.Repo,
      {Interruptus, repo: MinimalHostApp.Repo}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
