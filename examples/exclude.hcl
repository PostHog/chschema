# Example exclude config for `hclexp introspect` / `dump-cluster` / `diff` /
# `plan` / `drift`, all of which take `-exclude <this file>`. The comparison
# commands filter BOTH sides before diffing, so an excluded object appears in no
# output and no count.
#
# Objects whose name matches any glob (filepath.Match syntax: * ? [..]) are
# skipped before their DDL is parsed, so transient tables/dictionaries neither
# land in the dump nor break introspection when their DDL can't be parsed.
#
# A pattern is matched against both the bare object name ("tmp_foo") and the
# database-qualified form ("posthog.tmp_foo"), so you can scope a pattern to one
# database by including the "db." prefix.
#
# Tune to your fleet. The patterns below cover the transient objects observed in
# PostHog production dumps (ClickHouse atomic-replace temporaries, migration and
# DAG scratch tables, backups, staging, backfills).

exclude {
  patterns = [
    # ClickHouse atomic CREATE-OR-REPLACE / EXCHANGE temporaries
    "_tmp_replace_*",

    # Migration / DAG / ORM scratch tables (tmp_person_0007, tmp_dag_team_*, …)
    "tmp_*",
    "*_tmp",
    "infi_clickhouse_orm_migrations*",

    # Backups
    "*_backup",
    "*_backup_*",

    # Temp / backfill / staging
    "*_temp",
    "*_temp_*",
    "*_staging",
    "*_backfill",

    # Uncomment if your fleet uses these for transient promote/retire copies —
    # but beware of intentionally-kept tables (e.g. query_log_archive_old):
    # "*_old",
    # "*_new",
  ]

  # object_types drops a whole object class regardless of name — e.g. for a gate
  # that manages named collections out of band:
  #   object_types = ["named_collection"]
  #
  # Valid: table, materialized_view, view, dictionary, raw, named_collection.
}
