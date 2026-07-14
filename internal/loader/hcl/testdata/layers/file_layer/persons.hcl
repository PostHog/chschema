database "posthog" {
  table "persons" {
    column "id" { type = "UUID" }
    engine "merge_tree" {}
    order_by = ["id"]
  }
}
