defmodule Interruptus.Repo.Migrations.AddInterruptusCompensation do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down(version: 2)
end
