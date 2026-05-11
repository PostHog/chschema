database "posthog" {
  table "a" {
    extend = "b"
    column "x" { type = "UInt64" }
    engine "merge_tree" {}
  }

  table "b" {
    extend = "a"
    column "y" { type = "UInt64" }
    engine "merge_tree" {}
  }
}