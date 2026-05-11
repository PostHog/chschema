database "posthog" {
  table "_event_base" {
    abstract = true
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
  }

  table "events_local" {
    extend = "_event_base"
    column "event" { type = "String" }
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events_local"
      replica_name = "{replica}"
    }
    order_by = ["timestamp", "team_id"]
  }

  table "events_by_team" {
    extend   = "events_local"
    order_by = ["team_id", "timestamp"]
  }

  table "events_distributed" {
    extend = "events_local"
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "default"
      remote_table    = "events_local"
    }
  }
}