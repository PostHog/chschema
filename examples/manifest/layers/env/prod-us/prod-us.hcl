# prod-us adds a region column to the shared events table via patch_table.
database "posthog" {
  patch_table "events" {
    column "region" { type = "LowCardinality(String)" }
  }
}
