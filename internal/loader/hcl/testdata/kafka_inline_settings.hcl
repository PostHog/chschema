database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }
    engine "kafka" {
      broker_list          = "kafka:9092"
      topic_list           = "events"
      group_name           = "ch_events"
      format               = "JSONEachRow"
      num_consumers        = 4
      max_block_size       = 1048576
      commit_on_select     = false
      skip_broken_messages = 100
      handle_error_mode    = "stream"
      sasl_mechanism       = "PLAIN"
      sasl_username        = "ch"
      sasl_password        = "[set via override layer]"
      extra = {
        kafka_some_future_setting = "foo"
      }
    }
  }
}
