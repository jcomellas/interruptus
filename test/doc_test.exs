defmodule Interruptus.DocTest do
  use ExUnit.Case, async: true

  doctest Interruptus.Command
  doctest Interruptus.Policy.Restart
  doctest Interruptus.Policy.Rollback
  doctest Interruptus.Schemas.WorkflowInstance
  doctest Interruptus.Engine
end
