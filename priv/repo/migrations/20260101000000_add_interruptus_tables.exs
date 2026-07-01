defmodule Interruptus.Repo.Migrations.AddInterruptusTables do
  use Ecto.Migration

  def up, do: Interruptus.Migration.up()
  def down, do: Interruptus.Migration.down()
end
