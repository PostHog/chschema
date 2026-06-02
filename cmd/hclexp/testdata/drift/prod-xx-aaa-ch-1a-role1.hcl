node "prod-xx-aaa-ch-1a-role1" {
  macros = {
    hostClusterRole = "role1"
    replica         = "a"
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
