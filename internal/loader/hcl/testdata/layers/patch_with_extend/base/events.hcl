database "posthog" {
  table "_event_base" {
    abstract = true
    column "timestamp" { type = "DateTime" }
    column "team_id"   { type = "UInt64" }
  }

  table "events_local" {
    extend = "_event_base"
    engine "merge_tree" {}
    order_by = ["timestamp", "team_id"]
  }
}