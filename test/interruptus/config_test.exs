defmodule Interruptus.ConfigTest do
  use ExUnit.Case, async: false

  alias Interruptus.Config

  test "node_id defaults to node name plus a stable per-boot token" do
    config_a = Config.new(name: ConfigTestInstanceA, repo: Interruptus.Test.Repo, node_id: nil)
    config_b = Config.new(name: ConfigTestInstanceB, repo: Interruptus.Test.Repo, node_id: nil)

    assert String.starts_with?(config_a.node_id, "#{Node.self()}/")

    # The boot token is stable within a VM: two configs built independently
    # agree, so leases from this VM are always attributable to it.
    assert config_a.node_id == config_b.node_id

    [_node, token] = String.split(config_a.node_id, "/", parts: 2)
    assert byte_size(token) > 0
  end

  test "explicit node_id is preserved" do
    config = Config.new(name: ConfigTestInstanceC, repo: Interruptus.Test.Repo, node_id: "n1")
    assert config.node_id == "n1"
  end

  test "missing repo raises ArgumentError" do
    assert_raise ArgumentError, ~r/:repo/, fn ->
      Config.new(name: ConfigMissingRepo)
    end
  end

  test "process names derive from the instance name" do
    assert Config.supervisor_name(Interruptus) == Interruptus.Supervisor
    assert Config.registry_name(Interruptus) == Interruptus.Registry
    assert Config.runner_supervisor_name(Interruptus) == Interruptus.RunnerSupervisor
    assert Config.task_supervisor_name(Interruptus) == Interruptus.TaskSupervisor
    assert Config.recovery_name(Interruptus) == Interruptus.Recovery

    assert Config.registry_name(MyApp.Flows) == MyApp.Flows.Registry

    config = Config.new(name: MyApp.Flows, repo: Interruptus.Test.Repo)
    assert Config.registry_name(config) == MyApp.Flows.Registry
  end
end

defmodule Interruptus.WorkflowTypeTest do
  use ExUnit.Case, async: false

  alias Interruptus.WorkflowType

  test "resolves valid workflow modules" do
    assert {:ok, Interruptus.Test.Support.Workflows.Simple} =
             WorkflowType.resolve("Interruptus.Test.Support.Workflows.Simple")
  end

  test "rejects unknown module strings without minting atoms" do
    bogus = "No.Such.Module#{System.unique_integer([:positive])}"
    assert {:error, :unknown_workflow_type} = WorkflowType.resolve(bogus)

    last = bogus |> String.split(".") |> List.last()
    assert_raise ArgumentError, fn -> String.to_existing_atom("Elixir." <> last) end
  end

  test "rejects loaded modules that are not workflows" do
    assert {:error, :unknown_workflow_type} = WorkflowType.resolve("Enum")
  end
end
