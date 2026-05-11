database "posthog" {
  patch_table "_event_base" {
    column "us_session_id" { type = "String" }
  }
}