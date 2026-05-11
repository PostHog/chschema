database "posthog" {
  table "events" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
  patch_table "events" {
    column "extra" { type = "String" }
    engine "log" {}
  }
}