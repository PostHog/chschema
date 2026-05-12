database "posthog" {
  table "_event_base" {
    abstract = true
  }

  table "events" {
    extend = "_event_base"

    order_by      = ["timestamp", "team_id"]
    partition_by  = "toYYYYMM(timestamp)"
    sample_by     = "team_id"
    ttl           = "timestamp + INTERVAL 2 YEARS"
    settings      = {
      ttl_only_drop_parts = "1"
      index_granularity   = "8192"
    }

    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    column "event"     { type = "String" }

    index "idx_team" {
      expr        = "team_id"
      type        = "minmax"
      granularity = 4
    }

    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
  }
}