database "posthog" {
  table "events" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
  }
  patch_table "does_not_exist" {
    column "x" { type = "String" }
  }
}