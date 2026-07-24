defmodule Interruptus.Test.Repo.Migrations.AddInterruptusHardeningV5 do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up(version: 5)
  def down, do: Interruptus.Migration.down(version: 4)
end
