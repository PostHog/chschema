# Manifest example — multiple roles × environments

A worked, runnable example of a **manifest**: the role/env/layers file that
`hclexp web -manifest` and `hclexp plan -manifest` consume to compose and browse
(or diff) a whole fleet of schemas at once.

It models **two node roles** (`ops`, `data`) across **three environments**
(`dev`, `prod-us`, `prod-eu`) — six composed schemas in total.

## Layout

```
manifest.hcl              # role → env → ordered layer dirs
layers/
  base/events.hcl         # shared by every role+env: events, events_daily, events_daily_mv
  ops/ops.hcl             # OPS role only: system_metrics
  env/
    dev/dev.hcl           # dev only: debug_events scratch table
    prod-us/prod-us.hcl   # prod-us: patch_table events → + region
    prod-eu/prod-eu.hcl   # prod-eu: patch_table events → + region, + gdpr_redacted
```

Each `(env, role)` is the ordered composition of layer dirs:

| Env / Role | Layers | Distinctive objects |
| ---------- | ------ | ------------------- |
| `*` / ops  | `base` + `ops` + `env/<env>` | adds `system_metrics` |
| `*` / data | `base` + `env/<env>`         | no `system_metrics` |
| dev / *    | … + `env/dev`     | adds `debug_events`; `events` has no `region` |
| prod-us / *| … + `env/prod-us` | `events` gains `region` |
| prod-eu / *| … + `env/prod-eu` | `events` gains `region` + `gdpr_redacted` |

So the **role** axis decides whether `system_metrics` is present, and the
**env** axis decides the `events` columns and the dev-only scratch table —
visible side by side in the browser.

## Browse the whole fleet (`web -manifest`)

```bash
cd examples/manifest
hclexp web -manifest manifest.hcl -layer-root .
# open http://localhost:8080/  → a schema list grouped by env;
# each schema is browsable under /s/<env>/<role>/
```

Filter to one environment with `-env`:

```bash
hclexp web -manifest manifest.hcl -layer-root . -env prod-us
```

Edits to any layer file are picked up automatically (see `-reload-interval`).

## Inspect a single composition (`load` / `validate`)

The manifest is just sugar over layer stacks, so you can resolve any one
`(env, role)` directly:

```bash
# prod-us / ops — note `events` has the patched-in region column
hclexp load     -layer layers/base,layers/ops,layers/env/prod-us
hclexp validate -layer layers/base,layers/ops,layers/env/prod-us
```

## Generate a migration across roles (`plan`)

The same manifest feeds `hclexp plan`, which diffs every role for an environment
against a live-topology dump and emits one globally-ordered, cross-role migration
(storage before its proxies before the MV). See
[Cross-role planning](../../docs/README.hcl.md#cross-role-planning--hclexp-plan):

```bash
hclexp plan -manifest manifest.hcl -env prod-us -layer-root . -dump <topology-dir> -format json
```

(`plan` needs a `-dump` of the current cluster state; `web`/`load`/`validate`
above need only these files.)
