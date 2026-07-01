ExUnit.start()

{:ok, _} = Interruptus.Test.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(Interruptus.Test.Repo, :manual)

Interruptus.Config.new(repo: Interruptus.Test.Repo) |> Interruptus.Config.put()
