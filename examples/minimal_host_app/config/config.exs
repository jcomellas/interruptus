import Config

config :minimal_host_app, ecto_repos: [MinimalHostApp.Repo]

config :minimal_host_app, MinimalHostApp.Repo,
  database: "minimal_host_app_dev",
  username: "postgres",
  password: "postgres",
  hostname: "localhost"

config :interruptus, Interruptus,
  repo: MinimalHostApp.Repo,
  node_id: "minimal-host-1"
