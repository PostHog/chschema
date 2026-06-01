package test

import (
	"flag"
	"os"
	"strconv"
)

// clickhouse gates live tests that need a running ClickHouse instance.
// Defaults from the ENABLE_CLICKHOUSE env var so CI can opt in without
// the custom-flag-after-packages ordering trap.
var clickhouse = flag.Bool("clickhouse", envBoolClickhouse(), "run ClickHouse tests")

func envBoolClickhouse() bool {
	v, _ := strconv.ParseBool(os.Getenv("ENABLE_CLICKHOUSE"))
	return v
}
