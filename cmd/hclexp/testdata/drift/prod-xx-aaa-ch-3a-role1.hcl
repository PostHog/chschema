node "prod-xx-aaa-ch-3a-role1" {
  macros = {
    hostClusterRole = "role1"
    replica         = "a"
    shard           = "3"
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

  table "extra" {
    column "id" {
      type = "Int64"
    }

    engine "merge_tree" {
    }

    order_by = ["id"]
  }
}
