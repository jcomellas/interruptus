defmodule MinimalHostApp.Repo.Migrations.AddInterruptus do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down()
end
