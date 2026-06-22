database "posthog" {
  cluster = "posthog"

  table "_event_base" {
    abstract = true
  }

  table "events" {
    extend = "_event_base"

    comment       = "all product analytics events"
    primary_key   = ["team_id", "timestamp"]
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

    # column modifiers: every one must survive the dump round-trip (issue #45)
    column "created_at" {
      type    = "DateTime"
      default = "now()"
      codec   = "Delta, ZSTD(1)"
      ttl     = "created_at + INTERVAL 1 YEAR"
      comment = "ingestion time"
    }
    column "event_count" {
      type         = "UInt64"
      materialized = "1"
    }
    column "raw_payload" {
      type      = "String"
      ephemeral = ""
    }
    column "team_id_alias" {
      type  = "UInt64"
      alias = "team_id"
    }
    column "maybe_value" {
      type     = "Float64"
      nullable = true
    }

    index "idx_team" {
      expr        = "team_id"
      type        = "minmax"
      granularity = 4
    }

    constraint "team_id_positive" {
      check = "team_id > 0"
    }
    constraint "event_assumed_nonempty" {
      assume = "event != ''"
    }

    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
  }
}
