database "posthog" {
  table "events" {
    override = true
    column "dev_id" { type = "UInt32" }
    engine "log" {}
  }
}