database "posthog" {
  raw "tabel" "oops" {
    sql = "CREATE TABLE posthog.oops (id UInt64) ENGINE = MergeTree ORDER BY id"
  }
}
