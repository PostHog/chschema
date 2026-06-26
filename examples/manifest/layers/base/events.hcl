# Shared by every role and environment: the events table, a daily rollup, and
# the materialized view that fills it. The MV reads the local events table, so
# every composed schema resolves on its own.
database "posthog" {
  table "events" {
    engine "merge_tree" {}
    order_by = ["team_id", "timestamp"]
    column "uuid"       { type = "UUID" }
    column "team_id"    { type = "Int64" }
    column "timestamp"  { type = "DateTime" }
    column "event"      { type = "String" }
    column "properties" { type = "String" }
  }

  table "events_daily" {
    engine "merge_tree" {}
    order_by = ["team_id", "day"]
    column "team_id" { type = "Int64" }
    column "day"     { type = "Date" }
    column "events"  { type = "UInt64" }
  }

  materialized_view "events_daily_mv" {
    to_table = "posthog.events_daily"
    query    = "SELECT team_id, toDate(timestamp) AS day, count() AS events FROM posthog.events GROUP BY team_id, day"
  }
}
