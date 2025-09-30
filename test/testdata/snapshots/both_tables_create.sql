CREATE TABLE default.events (
  id UUID,
  subject_id UUID,
  event String,
  timestamp DateTime,
  properties JSON,
  inserted_at DateTime
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
 ORDER BY (event, timestamp)
 PARTITION BY toYYYYMM(timestamp)
 TTL timestamp + INTERVAL 2 YEARS
 SETTINGS ttl_only_drop_parts = 1;
CREATE TABLE default.users (
  id UInt64,
  email String,
  created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/users', '{replica}')
 ORDER BY (id)
