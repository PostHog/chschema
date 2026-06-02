node "prod-xx-aaa-ch-1a-role2" {
  macros = {
    hostClusterRole = "role2"
    replica         = "a"
    shard           = "1"
  }
}

database "posthog" {
  table "logs" {
    column "ts" {
      type = "DateTime"
    }

    engine "merge_tree" {
    }

    order_by = ["ts"]
  }
}
