database "posthog" {
  table "child" {
    extend = "does_not_exist"
    column "x" { type = "UInt64" }
    engine "merge_tree" {}
  }
}