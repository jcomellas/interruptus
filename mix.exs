defmodule Interruptus.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/erlar/interruptus"

  def project do
    [
      app: :interruptus,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :telemetry],
      mod: {Interruptus.Application, []}
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.11"},
      {:decimal, "~> 3.0"},
      {:postgrex, "~> 0.22", only: [:dev, :test]},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:ex_doc, "~> 0.39", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  # Test.Repo is only configured in config/test.exs; aliases that touch the
  # database or run ExUnit must use :test even when invoked from another alias
  # (e.g. mix precommit in the default :dev environment).
  # defp preferred_cli_env do
  def cli do
    [
      preferred_envs: [
        precommit: :test,
        test: :test,
        "test.setup": :test,
        setup: :test,
        "ecto.setup": :test,
        "ecto.reset": :test
      ]
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      "test.setup": ["ecto.create --quiet", "ecto.migrate --quiet"],
      test: ["test.setup", "test"],
      precommit: ["compile --warnings-as-errors", "deps.unlock --check-unused", "format", "test"]
    ]
  end

  defp package do
    [
      name: "interruptus",
      files: ~w(lib mix.exs README.md LICENSE .formatter.exs),
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "DESIGN.md", "AGENTS.md"],
      source_url: @source_url
    ]
  end
end
