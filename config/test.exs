import Config

config :interruptus, ecto_repos: [Interruptus.Test.Repo]

config :interruptus, Interruptus,
  repo: Interruptus.Test.Repo,
  prefix: "public",
  node_id: "test-node",
  lease_duration: 5_000,
  heartbeat_interval: 2_000,
  recovery_interval: 1_000,
  # Tests drive recovery explicitly via Interruptus.Recovery.recover_all/1.
  recovery_schedule: false

config :interruptus, Interruptus.Test.Repo,
  database: "interruptus_test",
  username: "interruptus",
  password: "interruptus",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox

config :logger, level: :warning
