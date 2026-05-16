named_collection "my_kafka" {
  param "kafka_broker_list" { value = "kafka:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "g1" }
  param "kafka_format"      { value = "JSONEachRow" }
}

database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" { collection = "my_kafka" }
  }
}
