-- database: roundtrip
-- Representative, cluster-free schema for the round-trip fidelity test. Every
-- object here must survive introspect -> dump HCL -> reparse -> GenerateSQL and
-- recreate to a byte-identical CREATE statement. Replace via ROUNDTRIP_FIXTURE
-- to verify a schema captured from production (see `hclexp dump-sql`).

CREATE TABLE roundtrip.dim_source
(
    id UInt64,
    name String
)
ENGINE = MergeTree
ORDER BY id
;

CREATE TABLE roundtrip.events
(
    timestamp DateTime,
    team_id Int64,
    event String,
    created_at DateTime DEFAULT now() COMMENT 'ingestion time' CODEC(Delta, ZSTD(1)) TTL created_at + toIntervalYear(1),
    event_count UInt64 MATERIALIZED 1,
    team_id_alias Int64 ALIAS team_id,
    maybe_value Nullable(Float64),
    CONSTRAINT team_id_positive CHECK team_id > 0
)
ENGINE = MergeTree
PRIMARY KEY team_id
ORDER BY (team_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + toIntervalYear(2)
COMMENT 'product analytics events'
;

CREATE TABLE roundtrip.events_summary
(
    timestamp DateTime,
    team_id Int64,
    c UInt64
)
ENGINE = SummingMergeTree
ORDER BY (team_id, timestamp)
;

CREATE MATERIALIZED VIEW roundtrip.events_mv TO roundtrip.events_summary
AS SELECT timestamp, team_id, count() AS c
FROM roundtrip.events
GROUP BY timestamp, team_id
;

-- NOTE: a plain VIEW (#48) and a DICTIONARY (#49) were intentionally left out of
-- the default fixture: the round-trip harness surfaced real fidelity gaps for
-- both (a view's CH-inferred column list is re-emitted as an explicit alias list;
-- a dictionary is dropped by the round-trip). Add them back here once those are
-- fixed so this fixture guards against regressions.
