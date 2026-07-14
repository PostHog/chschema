-- Not an HCL layer: naming this file as a layer entry must fail loudly.
CREATE TABLE posthog.seed (id UInt64) ENGINE = MergeTree ORDER BY id;
