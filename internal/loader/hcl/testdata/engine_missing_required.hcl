database "posthog" {
  table "t_bad" {
    column "id" { type = "UUID" }
    engine "replicated_merge_tree" {
      zoo_path = "/clickhouse/tables/{shard}/t_bad"
      // replica_name is required but missing
    }
  }
}