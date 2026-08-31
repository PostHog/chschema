named_collection "my_kafka" {
  external = true
}

database "posthog" {
  table "events_kafka" {
    column "team_id" { type = "Int64" }
    column "payload" { type = "String" }

    engine "kafka" {
      collection          = "my_kafka"
      topic_list          = "events"
      group_name          = "ch_events"
      format              = "JSONEachRow"
      num_consumers       = 4
      thread_per_consumer = true
    }
  }
}

