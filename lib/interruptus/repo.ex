defmodule Interruptus.Repo do
  @moduledoc """
  Wrapper around the host application's `Ecto.Repo`.

  Resolves repo module, PostgreSQL `prefix`, and passes through to the host
  repo configured in `Interruptus.Config`. All Interruptus persistence goes
  through this module.
  """

  alias Interruptus.Config

  @doc """
  Runs a zero-arity function inside a database transaction.

  ## Arguments

    * `config` - Interruptus config with `:repo`
    * `fun` - function returning `{:ok, term()}`, `{:error, term()}`, or a bare value
    * `opts` - passed to the host repo's `transaction/2`

  ## Returns

    * `{:ok, term()}` - transaction committed successfully
    * `{:error, term()}` - transaction rolled back
  """
  @spec transaction(Config.t(), (-> any()), keyword()) :: {:ok, any()} | {:error, any()}
  def transaction(%Config{repo: repo} = _config, fun, opts \\ []) when is_function(fun, 0) do
    repo.transaction(fun, opts)
  end

  @doc """
  Inserts an Ecto struct using the configured repo and prefix.

  ## Arguments

    * `config` - Interruptus config
    * `struct` - schema struct to insert
    * `opts` - passed to host repo `insert/2` (merged with prefix)

  ## Returns

    * `{:ok, struct}` - inserted struct
    * `{:error, %Ecto.Changeset{}}` - validation failure
  """
  @spec insert(Config.t(), Ecto.Schema.t(), keyword()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def insert(config, struct, opts \\ []) do
    config.repo.insert(struct, repo_opts(config, opts))
  end

  @doc """
  Updates an Ecto struct using the configured repo and prefix.

  ## Arguments

    * `config` - Interruptus config
    * `struct` - schema struct to update
    * `opts` - passed to host repo `update/2`

  ## Returns

    * `{:ok, struct}` - updated struct
    * `{:error, %Ecto.Changeset{}}` - validation failure
  """
  @spec update(Config.t(), Ecto.Schema.t(), keyword()) ::
          {:ok, Ecto.Schema.t()} | {:error, Ecto.Changeset.t()}
  def update(config, struct, opts \\ []) do
    config.repo.update(struct, repo_opts(config, opts))
  end

  @doc """
  Fetches a single result or returns nil.

  ## Arguments

    * `config` - Interruptus config
    * `queryable` - Ecto query or schema
    * `opts` - passed to host repo `one/2`

  ## Returns

    * Struct or `nil`
  """
  @spec one(Config.t(), Ecto.Queryable.t(), keyword()) :: Ecto.Schema.t() | nil
  def one(config, queryable, opts \\ []) do
    config.repo.one(queryable, repo_opts(config, opts))
  end

  @doc """
  Fetches a single result or raises.

  ## Arguments

    * `config` - Interruptus config
    * `queryable` - Ecto query or schema
    * `opts` - passed to host repo `one!/2`

  ## Returns

    * Struct

  ## Raises

    * `Ecto.NoResultsError` - when no row matches the query
  """
  @spec one!(Config.t(), Ecto.Queryable.t(), keyword()) :: Ecto.Schema.t()
  def one!(config, queryable, opts \\ []) do
    config.repo.one!(queryable, repo_opts(config, opts))
  end

  @doc """
  Returns all results for a query.

  ## Arguments

    * `config` - Interruptus config
    * `queryable` - Ecto query or schema
    * `opts` - passed to host repo `all/2`

  ## Returns

    * List of structs (possibly empty)
  """
  @spec all(Config.t(), Ecto.Queryable.t(), keyword()) :: [Ecto.Schema.t()]
  def all(config, queryable, opts \\ []) do
    config.repo.all(queryable, repo_opts(config, opts))
  end

  @doc """
  Executes a bulk update query.

  ## Arguments

    * `config` - Interruptus config
    * `queryable` - Ecto query identifying rows to update
    * `updates` - keyword list of `set:` fields for `update_all`
    * `opts` - passed to host repo `update_all/3`

  ## Returns

    * `{count, nil}` - number of rows affected
  """
  @spec update_all(Config.t(), Ecto.Queryable.t(), keyword(), keyword()) ::
          {non_neg_integer(), nil}
  def update_all(config, queryable, updates, opts \\ []) do
    config.repo.update_all(queryable, updates, repo_opts(config, opts))
  end

  @doc """
  Executes a raw SQL query.

  ## Arguments

    * `config` - Interruptus config
    * `sql` - SQL string with placeholders
    * `params` - query parameters
    * `opts` - passed to host repo `query!/3`

  ## Returns

    * `%Ecto.Adapters.SQL.Result{}`

  ## Raises

    * `Postgrex.Error` or other DB errors - when SQL execution fails
  """
  @spec query!(Config.t(), String.t(), list(), keyword()) :: map()
  def query!(config, sql, params \\ [], opts \\ []) do
    config.repo.query!(sql, params, repo_opts(config, opts))
  end

  @doc """
  Merges the configured PostgreSQL prefix into repo options.

  ## Arguments

    * `config` - Interruptus config with `:prefix`
    * `opts` - existing repo options

  ## Returns

    * Keyword list with `:prefix` set
  """
  @spec repo_opts(Config.t(), keyword()) :: keyword()
  def repo_opts(%Config{prefix: prefix}, opts) do
    Keyword.merge([prefix: prefix], opts)
  end
end
