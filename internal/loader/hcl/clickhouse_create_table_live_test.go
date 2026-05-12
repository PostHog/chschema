package hcl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCHLive_CreateTable executes every non-skipLive createTableCase against
// a real ClickHouse instance and asserts that the generated DDL is accepted
// by the server. Gated by the -clickhouse flag; requires a running
// ClickHouse (e.g. via `docker compose up -d`).
//
// Each case shares a single isolated database (auto-created and cleaned up
// by testhelpers.CreateTestDatabase). Tables are dropped after every
// subtest so the cases don't collide on the `t` table name.
func TestCHLive_CreateTable(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)

	for _, tc := range createTableCases {
		tc := tc
		if tc.skipLive {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				_ = conn.Exec(context.Background(),
					fmt.Sprintf("DROP TABLE IF EXISTS %s.t", dbName))
			})

			// Rewrite the fixture's `database "db"` block to point at the
			// isolated test database. Every fixture uses this exact literal,
			// so a single Replace is sufficient.
			src := strings.Replace(tc.hcl, `database "db"`, fmt.Sprintf(`database %q`, dbName), 1)
			sql := parseAndGenerate(t, src)

			err := conn.Exec(context.Background(), sql)
			require.NoError(t, err, "CREATE TABLE rejected by ClickHouse.\nGenerated SQL:\n%s", sql)

			// Sanity check: the table actually exists post-create.
			var got string
			row := conn.QueryRow(context.Background(),
				fmt.Sprintf("SELECT name FROM system.tables WHERE database = '%s' AND name = 't'", dbName))
			require.NoError(t, row.Scan(&got))
			assert.Equal(t, "t", got)
		})
	}
}
