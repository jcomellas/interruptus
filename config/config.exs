import Config

config :interruptus, ecto_repos: [Interruptus.Test.Repo]

config :interruptus, Interruptus,
  repo: nil,
  prefix: "public",
  node_id: "dev-node",
  lease_duration: 30_000,
  heartbeat_interval: 10_000,
  recovery_interval: 5_000

import_config "#{config_env()}.exs"
