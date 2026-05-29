database "default" {
  cluster = "main"

  table "events_base" {
    abstract = true
    column "timestamp"  { type = "DateTime64(3)" }
    column "team_id"    { type = "UInt32" }
    column "event"      { type = "String" }
    column "properties" {
      type  = "String"
      codec = "ZSTD(3)"
    }
  }

  table "events_kafka" {
    extend = "events_base"
    engine "kafka" {
      broker_list = "kafka:9092"
      topic_list  = "events"
      group_name  = "ch_events"
      format      = "JSONEachRow"
    }
  }

  table "events_local" {
    extend = "events_base"
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/events"
      replica_name = "{replica}"
    }
    order_by     = ["team_id", "timestamp"]
    partition_by = "toYYYYMM(timestamp)"
  }

  table "events_distributed" {
    extend = "events_base"
    engine "distributed" {
      cluster_name    = "main"
      remote_database = "default"
      remote_table    = "events_local"
      sharding_key    = "cityHash64(team_id)"
    }
  }

  materialized_view "events_mv" {
    extend   = "events_base"
    to_table = "events_local"
    query    = "SELECT timestamp, team_id, event, properties FROM events_kafka"
  }
}
