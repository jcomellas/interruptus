defmodule Interruptus.Repo.Migrations.AddInterruptusEffects do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down(version: 1)
end
