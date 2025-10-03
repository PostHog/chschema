CREATE TABLE default.events (
  `id` UUID,
  `subject_id` UUID,
  `event` String,
  `timestamp` DateTime,
  `properties` JSON,
  `inserted_at` DateTime
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
 PARTITION BY toYYYYMM(timestamp)
 ORDER BY (event, timestamp)
 TTL timestamp + INTERVAL 2 YEARS
 SETTINGS ttl_only_drop_parts = 1