database "posthog" {
  table "a" {
    extend = "a"
    column "x" { type = "UInt64" }
    engine "merge_tree" {}
  }
}