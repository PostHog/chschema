package main

import (
	"context"
	"flag"
	"net"
	"net/url"
	"strconv"
	"testing"

	"github.com/posthog/chschema/config"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var diffClickHouseLive = flag.Bool("clickhouse", false, "run diff live-side tests against ClickHouse")

func TestDiffLiveSide_IntrospectionControls(t *testing.T) {
	if !*diffClickHouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	dbName := testhelpers.CreateTestDatabase(t, conn)
	ctx := context.Background()

	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+dbName+
		".events (id UInt64) ENGINE = MergeTree ORDER BY id"))
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+dbName+
		".tmp_unsupported (id UInt64) ENGINE = StripeLog"))

	uri := diffLiveClickHouseURI(dbName)

	t.Run("strict remains the default", func(t *testing.T) {
		_, err := loadSideWithOptions(uri, diffSideLoadOptions{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported engine")
		assert.Contains(t, err.Error(), "StripeLog")
	})

	t.Run("exclude skips before parsing", func(t *testing.T) {
		schema, err := loadSideWithOptions(uri, diffSideLoadOptions{
			exclude: hclload.NewExcludeMatcher("tmp_*"),
		})
		require.NoError(t, err)
		require.Len(t, schema.Databases, 1)
		assert.Equal(t, []string{"events"}, diffTableNames(schema.Databases[0].Tables))
		assert.Empty(t, schema.Databases[0].Raws)
	})

	t.Run("allow raw captures and continues", func(t *testing.T) {
		schema, err := loadSideWithOptions(uri, diffSideLoadOptions{allowRaw: true})
		require.NoError(t, err)
		require.Len(t, schema.Databases, 1)
		assert.Equal(t, []string{"events"}, diffTableNames(schema.Databases[0].Tables))
		require.Len(t, schema.Databases[0].Raws, 1)
		assert.Equal(t, "tmp_unsupported", schema.Databases[0].Raws[0].Name)
		assert.Equal(t, "table", schema.Databases[0].Raws[0].Kind)
		assert.Contains(t, schema.Databases[0].Raws[0].SQL, "ENGINE = StripeLog")
	})
}

func diffLiveClickHouseURI(database string) string {
	cfg := config.GetDefaultConfig()
	u := url.URL{
		Scheme: "clickhouse",
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + database,
	}
	if cfg.User != "" || cfg.Password != "" {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}
	query := u.Query()
	if cfg.Secure {
		query.Set("secure", "true")
	}
	if cfg.TLSSkipVerify {
		query.Set("skip-verify", "true")
	}
	u.RawQuery = query.Encode()
	return u.String()
}

func diffTableNames(tables []hclload.TableSpec) []string {
	names := make([]string, len(tables))
	for i := range tables {
		names[i] = tables[i].Name
	}
	return names
}
