database "posthog" {
  table "events" {
    column "dev_id" { type = "UInt32" }
    engine "log" {}
  }
}