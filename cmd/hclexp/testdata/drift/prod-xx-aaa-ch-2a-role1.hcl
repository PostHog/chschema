node "prod-xx-aaa-ch-2a-role1" {
  macros = {
    hostClusterRole = "role1"
    replica         = "a"
    shard           = "2"
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
