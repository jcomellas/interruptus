locals_without_parens = [
  workflow: 1,
  param: 2,
  param: 3,
  data: 2,
  data: 3,
  pipeline: 1,
  checkpoint: 1,
  verify: 1,
  restart_policy: 1,
  rollback_policy: 1,
  pipeline_version: 1
]

[
  line_length: 120,
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens],
  import_deps: [:ecto, :ecto_sql],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
