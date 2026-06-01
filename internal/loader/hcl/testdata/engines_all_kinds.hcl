database "posthog" {
  table "t_merge_tree" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }

  table "t_replicated_merge_tree" {
    column "id" { type = "UUID" }
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/t_replicated_merge_tree"
      replica_name = "{replica}"
    }
  }

  table "t_replacing_merge_tree" {
    column "id" { type = "UUID" }
    engine "replacing_merge_tree" {
      version_column = "ver"
    }
  }

  table "t_replicated_replacing_merge_tree" {
    column "id" { type = "UUID" }
    engine "replicated_replacing_merge_tree" {
      zoo_path       = "/clickhouse/tables/{shard}/t_rrmt"
      replica_name   = "{replica}"
      version_column = "ver"
    }
  }

  table "t_summing_merge_tree" {
    column "id" { type = "UUID" }
    engine "summing_merge_tree" {
      sum_columns = ["a", "b"]
    }
  }

  table "t_replicated_summing_merge_tree" {
    column "id" { type = "UUID" }
    engine "replicated_summing_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/t_rsmt"
      replica_name = "{replica}"
      sum_columns  = ["a", "b"]
    }
  }

  table "t_collapsing_merge_tree" {
    column "id" { type = "UUID" }
    engine "collapsing_merge_tree" {
      sign_column = "sign"
    }
  }

  table "t_replicated_collapsing_merge_tree" {
    column "id" { type = "UUID" }
    engine "replicated_collapsing_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/t_rcmt"
      replica_name = "{replica}"
      sign_column  = "sign"
    }
  }

  table "t_aggregating_merge_tree" {
    column "id" { type = "UUID" }
    engine "aggregating_merge_tree" {}
  }

  table "t_replicated_aggregating_merge_tree" {
    column "id" { type = "UUID" }
    engine "replicated_aggregating_merge_tree" {
      zoo_path     = "/clickhouse/tables/{shard}/t_ramt"
      replica_name = "{replica}"
    }
  }

  table "t_distributed" {
    column "id" { type = "UUID" }
    engine "distributed" {
      cluster_name    = "posthog"
      remote_database = "default"
      remote_table    = "t_merge_tree"
      sharding_key    = "sipHash64(id)"
    }
  }

  table "t_log" {
    column "id" { type = "UUID" }
    engine "log" {}
  }

  table "t_kafka" {
    column "id" { type = "UUID" }
    engine "kafka" {
      broker_list = "kafka:9092"
      topic_list  = "events"
      group_name  = "ingest"
      format      = "JSONEachRow"
    }
  }

  table "t_join" {
    column "user_id" { type = "UInt64" }
    column "session_id" { type = "UInt64" }
    column "value" { type = "String" }
    engine "join" {
      strictness = "ANY"
      type       = "LEFT"
      keys       = ["user_id", "session_id"]
    }
  }

  table "t_null" {
    column "id" { type = "UUID" }
    engine "null" {}
  }

  table "t_memory" {
    column "id" { type = "UUID" }
    engine "memory" {}
  }

  table "t_merge" {
    column "id" { type = "UUID" }
    engine "merge" {
      db_regex    = "default"
      table_regex = "^shard_.*"
    }
  }

  table "t_buffer" {
    column "id" { type = "UUID" }
    engine "buffer" {
      database    = ""
      table       = "t_merge_tree"
      num_layers  = 16
      min_time    = 10
      max_time    = 100
      min_rows    = 10000
      max_rows    = 1000000
      min_bytes   = 10000000
      max_bytes   = 100000000
      flush_time  = 30
    }
  }

  table "t_time_series" {
    column "metric_name" { type = "LowCardinality(String)" }
    engine "time_series" {
      settings = { id_generator = "sipHash64(metric_name, all_tags)" }
      tags_to_columns = { instance = "instance", job = "job" }
      samples { target = "default.t_time_series_data" }
      tags { target = "default.t_time_series_tags" }
      metrics { target = "default.t_time_series_metrics" }
    }
  }
}