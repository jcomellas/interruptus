defmodule MinimalHostApp.Repo do
  use Ecto.Repo,
    otp_app: :minimal_host_app,
    adapter: Ecto.Adapters.Postgres
end
