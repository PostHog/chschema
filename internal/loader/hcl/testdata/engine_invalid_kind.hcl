database "posthog" {
  table "t_bad" {
    column "id" { type = "UUID" }
    engine "not_a_real_kind" {}
  }
}