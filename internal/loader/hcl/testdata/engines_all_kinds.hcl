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
      broker_list    = ["kafka:9092"]
      topic          = "events"
      consumer_group = "ingest"
      format         = "JSONEachRow"
    }
  }
}