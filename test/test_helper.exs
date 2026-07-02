ExUnit.start()

# Integration tests share a single OTP tree (Registry, runners, Recovery) and
# named support processes. Running them concurrently across modules causes
# intermittent Registry / sandbox failures.
ExUnit.configure(max_cases: 1)

{:ok, _} = Interruptus.Test.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(Interruptus.Test.Repo, :manual)

Interruptus.Config.new(repo: Interruptus.Test.Repo) |> Interruptus.Config.put()

{:ok, _} = Interruptus.Test.Support.Runtime.SupervisorLock.start_link()
:ok = Interruptus.Test.Support.Runtime.start!()

for module <- [
      Interruptus.Test.Support.ApprovalState,
      Interruptus.Test.Support.Barrier,
      Interruptus.Test.Support.InvocationCounter,
      Interruptus.Test.Support.CompensateOrder,
      Interruptus.Test.Support.VerifyState
    ] do
  case module.start_link() do
    {:ok, _} -> :ok
    {:error, {:already_started, _}} -> :ok
  end
end
