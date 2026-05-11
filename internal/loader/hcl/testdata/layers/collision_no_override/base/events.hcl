database "posthog" {
  table "events" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
}