database "posthog" {
  view "v" {
    query        = "SELECT 1"
    sql_security = "bogus"
  }
}
