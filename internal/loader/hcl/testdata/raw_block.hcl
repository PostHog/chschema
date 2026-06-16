database "posthog" {
  raw "dictionary" "city_postal_ip_trie" {
    sql = <<-SQL
      CREATE DICTIONARY posthog.city_postal_ip_trie (`prefix` String)
      PRIMARY KEY prefix
      SOURCE(CLICKHOUSE(USER 'admin'))
      LIFETIME(MIN 0 MAX 3600)
      LAYOUT(IP_TRIE)
    SQL
  }
}
