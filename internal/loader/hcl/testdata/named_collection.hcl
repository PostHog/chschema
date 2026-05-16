named_collection "my_kafka" {
  cluster = "posthog"
  comment = "shared kafka cluster for events ingestion"

  param "kafka_broker_list" { value = "k1:9092,k2:9092" }
  param "kafka_topic_list"  { value = "events" }
  param "kafka_group_name"  { value = "ch_events" }
  param "kafka_format"      { value = "JSONEachRow" }
  param "kafka_sasl_password" {
    value       = "[set via override layer]"
    overridable = false
  }
}

named_collection "external_xml_managed" {
  external = true
  comment  = "managed in /etc/clickhouse-server/config.d/kafka.xml"
}
