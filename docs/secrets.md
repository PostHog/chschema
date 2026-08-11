# Secrets in introspected schemas

ClickHouse stores secrets — dictionary source passwords, named-collection values
like `kafka_broker_list` — inside `create_table_query` / `SHOW CREATE` /
`system.named_collections`. By default it **redacts** them to the placeholder
`[HIDDEN]` when you read those, unless three conditions are all met:

1. the server is configured with `display_secrets_in_show_and_select = 1`
   (a server-level setting in `config.xml`, *not* settable per session);
2. the connecting user holds the `displaySecretsInShowAndSelect` privilege
   (`GRANT displaySecretsInShowAndSelect ON *.* TO <user>`); and
3. the query enables the `format_display_secrets_in_show_and_select` session
   setting.

## Default behavior: secrets are marked unknown, never overwritten

hclexp controls only #3 and leaves it **off by default**. With redaction on, a
captured secret comes back as `[HIDDEN]`. hclexp retains that marker in an HCL
dump so later comparisons can distinguish "a secret exists but is unknown" from
"there is no secret". It does **not** re-emit `[HIDDEN]`:

- dictionary `CREATE`/`CREATE OR REPLACE` is blocked when its target contains a
  marker;
- an altered dictionary is also blocked when its live source contains a marker
  and the same target source would omit that credential;
- named-collection params with redacted values are skipped by `diff`, while
  whole-collection creates/recreates containing them are blocked.

Blocked dictionary rewrites produce no DDL and are reported unsafe. This covers
both failure modes: writing the literal placeholder and rendering apparently
clean SQL that silently omits a real-but-hidden credential. A visible target
credential remains safe to write, including a deliberate rotation.

In authored HCL, use `password = "[HIDDEN]"` (or
`credentials_password = "[HIDDEN]"`) to declare a credential managed outside
hclexp. Simply omitting a live credential is still a real difference. If the
live value was redacted, hclexp will report it but refuse to remove it
automatically; reveal the secret during introspection or make that removal
manually.

## Capturing real secrets: `-show-secrets`

When you genuinely want real secret values in the output (for example, to
recreate a database on a fresh cluster), pass `-show-secrets` to `introspect` or
`dump-sql`:

```bash
hclexp introspect -host … -database posthog -show-secrets
hclexp dump-sql    -host … -database posthog -show-secrets -out posthog.sql
```

This enables `format_display_secrets_in_show_and_select = 1` on the connection
(condition #3). It only reveals secrets if the server and grant (conditions #1
and #2) also allow it; otherwise values stay `[HIDDEN]` and are dropped as usual.
The flag is always safe to pass — without the prerequisites it simply has no
effect.

To enable the prerequisites on a cluster you control:

```xml
<!-- /etc/clickhouse-server/config.d/secrets.xml -->
<clickhouse>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
</clickhouse>
```

```sql
GRANT displaySecretsInShowAndSelect ON *.* TO <user>;
```

> **Security warning:** `-show-secrets` writes real passwords and connection
> strings into the introspected HCL / SQL. Treat the output as sensitive — do not
> commit it to version control or share it. Leave the flag off for routine drift
> checks; use it only for a one-off capture you intend to handle securely.
