node "prod-us-iad-ch-1d-ops" {
  macros = {
    hostClusterRole = "ops"
    hostClusterType = "online"
    replica         = "d"
    shard           = "1"
  }
}

database "posthog" {
  table "events" {
    column "team_id" {
      type = "Int64"
    }

    engine "merge_tree" {
    }

    order_by = ["team_id"]
  }
}
