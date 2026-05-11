database "posthog" {
  table "events" {
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
    engine "merge_tree" {}
    order_by = ["timestamp", "team_id"]
  }
}