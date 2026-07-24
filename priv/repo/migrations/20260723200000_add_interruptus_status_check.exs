defmodule Interruptus.Repo.Migrations.AddInterruptusStatusCheck do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down(version: 3)
end
