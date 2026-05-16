database "posthog" {
  dictionary "exchange_rate_dict" {
    primary_key = ["currency"]
    settings    = { format_csv_allow_single_quotes = "1" }
    cluster     = "posthog"
    comment     = "fx rates by date"

    lifetime {
      min = 3000
      max = 3600
    }
    range {
      min = "start_date"
      max = "end_date"
    }

    attribute "currency"   { type = "String" }
    attribute "start_date" { type = "Date" }
    attribute "end_date"   { type = "Nullable(Date)" }
    attribute "rate"       { type = "Decimal64(10)" }

    source "clickhouse" {
      query    = "SELECT currency, start_date, end_date, rate FROM default.exchange_rate"
      user     = "default"
      password = "[HIDDEN]"
    }
    layout "complex_key_range_hashed" {
      range_lookup_strategy = "max"
    }
  }
}
