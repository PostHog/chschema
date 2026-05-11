database "posthog" {
  table "events" {
    not_a_real_attr = "oops"

    column "id" {
      type = "UUID"
    }
  }
}