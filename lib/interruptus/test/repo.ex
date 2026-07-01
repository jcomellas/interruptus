defmodule Interruptus.Test.Repo do
  @moduledoc """
  Test-only Ecto repo for Interruptus integration tests.

  Used by the test suite with SQL Sandbox. Host applications provide their own
  repo via `Interruptus.Config`; this module is not used in production.
  """

  use Ecto.Repo,
    otp_app: :interruptus,
    adapter: Ecto.Adapters.Postgres
end
