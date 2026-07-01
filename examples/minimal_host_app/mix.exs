defmodule MinimalHostApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :minimal_host_app,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {MinimalHostApp.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:interruptus, path: "../.."},
      {:ecto_sql, "~> 3.11"},
      {:postgrex, "~> 0.22"},
    ]
  end
end
