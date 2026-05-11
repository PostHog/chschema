database "posthog" {
  table "_base" {
    abstract = true
    column "x" { type = "UInt64" }
  }

  table "child" {
    extend = "_base"
    column "x" { type = "String" }
    engine "merge_tree" {}
  }
}