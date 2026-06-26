# prod-eu adds region plus a GDPR redaction flag.
database "posthog" {
  patch_table "events" {
    column "region"        { type = "LowCardinality(String)" }
    column "gdpr_redacted" { type = "UInt8" }
  }
}
