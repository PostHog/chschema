database "posthog" {
  table "_event_base" {
    abstract = true

    column "timestamp" {
      type    = "DateTime64(6)"
      comment = "ingest time"
    }
    column "team_id" {
      type = "UInt64"
    }
  }

  table "sharded_events" {
    extend = "_event_base"

    patch_column "timestamp" {
      codec = "Delta(8), ZSTD(1)"
    }
    patch_column "team_id" {
      codec = "T64, ZSTD(1)"
    }

    engine "merge_tree" {}
  }

  table "events" {
    extend = "_event_base"

    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
  }
}
