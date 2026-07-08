# Example manifest: two node roles (ops, data) across three environments
# (dev, prod-us, prod-eu). Each (env, role) is the ordered composition of layer
# directories — the same format `hclexp plan` and `hclexp web -manifest` consume.
#
#   ops  = base + ops + env/<env>     (ops nodes also run infra tables)
#   data = base +       env/<env>     (data nodes run just the shared schema)
#
# Layer paths are relative to -layer-root (run the examples below from this dir
# with `-layer-root .`).

role "ops" {
  env "dev"     { layers = ["layers/base", "layers/ops", "layers/env/dev"] }
  env "prod-us" { layers = ["layers/base", "layers/ops", "layers/env/prod-us"] }
  env "prod-eu" { layers = ["layers/base", "layers/ops", "layers/env/prod-eu"] }
}

role "data" {
  env "dev"     { layers = ["layers/base", "layers/env/dev"] }
  env "prod-us" { layers = ["layers/base", "layers/env/prod-us"] }
  env "prod-eu" { layers = ["layers/base", "layers/env/prod-eu"] }
}

# Optional cluster blocks (consumed by `hclexp validate -manifest`, ignored by
# plan/web). A ClickHouse cluster is composed of nodes from one or more roles;
# its schema is the union of those roles' compositions. `validate` resolves a
# Distributed table's cluster_name (and remote_servers aliases) against it.
cluster "ops" {
  roles = ["ops"]
}

cluster "posthog" {
  roles   = ["data"]
  aliases = ["posthog_writable"]
}
