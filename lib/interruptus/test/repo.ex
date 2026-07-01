defmodule Interruptus.Test.Repo do
  @moduledoc false

  use Ecto.Repo,
    otp_app: :interruptus,
    adapter: Ecto.Adapters.Postgres
end
