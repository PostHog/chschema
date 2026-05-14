database "posthog" {
  materialized_view "app_metrics_mv" {
    to_table = "default.sharded_app_metrics"
    query    = "SELECT team_id, category FROM default.kafka_app_metrics"
    cluster  = "posthog"
    comment  = "rolls metrics up"
    column "team_id"  { type = "Int64" }
    column "category" { type = "LowCardinality(String)" }
  }
}
