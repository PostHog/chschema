package hcl

import (
	"context"
	"testing"

	"github.com/posthog/chschema/test/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCHLive_IntrospectNode(t *testing.T) {
	if !*clickhouseLive {
		t.Skip("pass -clickhouse to run against a live ClickHouse")
	}
	conn := testhelpers.RequireClickHouse(t)
	ctx := context.Background()

	// Macros may be empty on a vanilla instance, but the query must succeed
	// and return a non-nil map.
	macros, err := IntrospectMacros(ctx, conn)
	require.NoError(t, err)
	assert.NotNil(t, macros)

	// hostName() always resolves, so a node with no override gets a name.
	node, err := IntrospectNode(ctx, conn, "")
	require.NoError(t, err)
	assert.NotEmpty(t, node.Name, "node name should fall back to hostName()")
	assert.Equal(t, macros, node.Macros)

	// An explicit override wins over hostName().
	named, err := IntrospectNode(ctx, conn, "prod-us-iad-ch-1d-ops")
	require.NoError(t, err)
	assert.Equal(t, "prod-us-iad-ch-1d-ops", named.Name)
}
