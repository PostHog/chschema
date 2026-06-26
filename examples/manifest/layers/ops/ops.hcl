# Only the OPS role runs the infra metrics table; the DATA role does not compose
# this layer.
database "posthog" {
  table "system_metrics" {
    engine "merge_tree" {}
    order_by = ["metric", "timestamp"]
    column "metric"    { type = "LowCardinality(String)" }
    column "timestamp" { type = "DateTime" }
    column "value"     { type = "Float64" }
  }
}
