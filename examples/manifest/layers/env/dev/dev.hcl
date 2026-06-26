# Dev-only: a scratch table that doesn't exist in prod.
database "posthog" {
  table "debug_events" {
    engine "merge_tree" {}
    order_by = ["timestamp"]
    column "timestamp" { type = "DateTime" }
    column "payload"   { type = "String" }
  }
}
