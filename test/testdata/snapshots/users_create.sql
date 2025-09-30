CREATE TABLE default.users (
  id UInt64,
  email String,
  created_at DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/users', '{replica}')
 ORDER BY (id)
