defmodule Interruptus.Repo do
  @moduledoc """
  Wrapper around the host application's `Ecto.Repo`.

  Resolves repo, prefix, and logging options from `Interruptus.Config`.
  """

  alias Interruptus.Config

  @doc """
  Runs a function inside a transaction on the configured repo.
  """
  @spec transaction(Config.t(), (-> any()), keyword()) :: {:ok, any()} | {:error, any()}
  def transaction(%Config{repo: repo} = _config, fun, opts \\ []) when is_function(fun, 0) do
    repo.transaction(fun, opts)
  end

  @doc """
  Inserts a struct using the configured repo and prefix.
  """
  @spec insert(Config.t(), Ecto.Schema.t(), keyword()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def insert(config, struct, opts \\ []) do
    config.repo.insert(struct, repo_opts(config, opts))
  end

  @doc """
  Updates a struct using the configured repo and prefix.
  """
  @spec update(Config.t(), Ecto.Schema.t(), keyword()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(config, struct, opts \\ []) do
    config.repo.update(struct, repo_opts(config, opts))
  end

  @doc """
  Fetches a single result or nil.
  """
  @spec one(Config.t(), Ecto.Queryable.t(), keyword()) :: Ecto.Schema.t() | nil
  def one(config, queryable, opts \\ []) do
    config.repo.one(queryable, repo_opts(config, opts))
  end

  @doc """
  Fetches a single result or raises.
  """
  @spec one!(Config.t(), Ecto.Queryable.t(), keyword()) :: Ecto.Schema.t()
  def one!(config, queryable, opts \\ []) do
    config.repo.one!(queryable, repo_opts(config, opts))
  end

  @doc """
  Returns all results for a query.
  """
  @spec all(Config.t(), Ecto.Queryable.t(), keyword()) :: [Ecto.Schema.t()]
  def all(config, queryable, opts \\ []) do
    config.repo.all(queryable, repo_opts(config, opts))
  end

  @doc """
  Executes a raw SQL query and returns the number of affected rows.
  """
  @spec update_all(Config.t(), Ecto.Queryable.t(), keyword(), keyword()) ::
          {non_neg_integer(), nil}
  def update_all(config, queryable, updates, opts \\ []) do
    config.repo.update_all(queryable, updates, repo_opts(config, opts))
  end

  @doc """
  Executes a raw SQL query.
  """
  @spec query!(Config.t(), String.t(), list(), keyword()) :: Ecto.Adapters.SQL.Result.t()
  def query!(config, sql, params \\ [], opts \\ []) do
    config.repo.query!(sql, params, repo_opts(config, opts))
  end

  @doc """
  Returns repo options with prefix applied.
  """
  @spec repo_opts(Config.t(), keyword()) :: keyword()
  def repo_opts(%Config{prefix: prefix}, opts) do
    Keyword.merge([prefix: prefix], opts)
  end
end
