PostHog ClickHouse setup

# Cluster setup

## Environments

There are 3 environments:

- Development
- US Production
- EU Production

## US Production

The main cluster is:
 - 8 worker nodes
   - 2 shards
   - 4 replicas

## EU Production

The main cluster is:
 - 24 worker nodes - all tables are defined on worker nodes
   - 8 shards
   - 3 replicas
 - 2 coordinator nodes - only non-sharded and distributed tables

Additionally, on k8s we have stateless nodes:

## ShuffleHog nodes

It shuffles data between Kafka partitions.

## Ingestion nodes

It ingests data from Kafka to a proper ClickHouse shard.
Ingestion nodes are part of the extended ClickHouse cluster

The way this is done:
 1. Create a writetable table, this is a Distributed engine table
 2. Create a table with Kafka engine
 3. Create a materialized view that reads from Kafka and writes to the writetable table

If a desination table is non-sharded, we pick only one node as data for Distrubuted table.

# General rules

> [!CAUTION]
> Do not use `ON CLUSTER` clause, it happen to cause a lot of issues and it is not compatible with our migration setup.

# CREATE / DROP

## Replicated, non-sharded tables

To create a replicated table, use proper Engine and Replication settings.

The table should be created on all nodes including coordinators.

## Replicated, sharded tables


## Distributed tables

## Views

## Materialized views

# Recreating tables

If a table or a view is deleted and created again as part of the same migration,
the deletion must be performed with `SYNC` modifier.

# ALTER

## Non-sharded tables

Modifying a table is done with `ALTER TABLE` command.
It should be done on one node only.

## Sharded tables

## Distributed tables