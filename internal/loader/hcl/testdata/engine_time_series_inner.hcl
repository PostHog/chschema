database "default" {
  table "prom_metrics_inner" {
    column "metric_name" { type = "LowCardinality(String)" }
    engine "time_series" {
      samples {
        inner {
          column "id" { type = "UUID" }
          column "timestamp" { type = "DateTime64(3)" }
          column "value" { type = "Float64" }
          engine "merge_tree" {}
          order_by = ["id", "timestamp"]
        }
      }
    }
  }
}
