database "posthog" {
  patch_table "events" {
    column "us_session_id" { type = "String" }
  }
}