package hcl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// proxyTable builds a Distributed proxy over remoteDB.processes with the
// given columns.
func proxyTable(remoteDB string, cols ...ColumnSpec) TableSpec {
	return TableSpec{
		Name:    "distributed_system_processes",
		Columns: cols,
		Engine: &EngineSpec{Kind: "distributed", Decoded: EngineDistributed{
			ClusterName:    "posthog",
			RemoteDatabase: remoteDB,
			RemoteTable:    "processes",
		}},
	}
}

func proxySchema(t TableSpec) *Schema {
	return &Schema{Databases: []DatabaseSpec{{Name: "posthog", Tables: []TableSpec{t}}}}
}

// A Distributed proxy over system.* compares columns subset-tolerantly: the
// server owns the full set and grows it across versions, so presence
// differences are not drift (#136 item 4) — in either direction, since Diff
// has no notion of which operand is live.
func TestDiff_SystemProxyColumnSubsetTolerated(t *testing.T) {
	declared := proxyTable("system",
		ColumnSpec{Name: "query_id", Type: "String"},
		ColumnSpec{Name: "user", Type: "String"},
	)
	live := proxyTable("system",
		ColumnSpec{Name: "query_id", Type: "String"},
		ColumnSpec{Name: "user", Type: "String"},
		ColumnSpec{Name: "client_agent", Type: "LowCardinality(String)"},
	)

	assert.True(t, Diff(proxySchema(declared), proxySchema(live)).IsEmpty(),
		"a live system proxy carrying server columns the layer omits is not drift")
	assert.True(t, Diff(proxySchema(live), proxySchema(declared)).IsEmpty(),
		"the reverse orientation is tolerated too")
}

// Columns declared on both sides still compare fully: a real type change on
// a system proxy surfaces as MODIFY COLUMN.
func TestDiff_SystemProxySharedColumnStillCompared(t *testing.T) {
	left := proxyTable("system", ColumnSpec{Name: "user", Type: "String"})
	right := proxyTable("system", ColumnSpec{Name: "user", Type: "LowCardinality(String)"})

	cs := Diff(proxySchema(left), proxySchema(right))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.Len(t, td.ModifyColumns, 1)
	assert.Equal(t, "user", td.ModifyColumns[0].Name)
	assert.Empty(t, td.AddColumns)
	assert.Empty(t, td.DropColumns)
}

// Non-system proxies keep exact column semantics: their remotes are
// layer-managed, so an extra column is real drift.
func TestDiff_NonSystemProxyStaysExact(t *testing.T) {
	declared := proxyTable("posthog", ColumnSpec{Name: "id", Type: "UInt64"})
	live := proxyTable("posthog",
		ColumnSpec{Name: "id", Type: "UInt64"},
		ColumnSpec{Name: "extra", Type: "String"},
	)

	cs := Diff(proxySchema(declared), proxySchema(live))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.Len(t, td.AddColumns, 1)
	assert.Equal(t, "extra", td.AddColumns[0].Name)
}

// Suppression needs both sides to be system proxies: an engine change means
// the table is being repurposed, and the full column diff must surface.
func TestDiff_SystemProxyEngineChangeNotSuppressed(t *testing.T) {
	proxy := proxyTable("system",
		ColumnSpec{Name: "user", Type: "String"},
		ColumnSpec{Name: "client_agent", Type: "String"},
	)
	plain := TableSpec{
		Name:    "distributed_system_processes",
		OrderBy: []string{"user"},
		Columns: []ColumnSpec{{Name: "user", Type: "String"}},
		Engine:  &EngineSpec{Kind: "merge_tree", Decoded: EngineMergeTree{}},
	}

	cs := Diff(proxySchema(proxy), proxySchema(plain))
	require.Len(t, cs.Databases, 1)
	require.Len(t, cs.Databases[0].AlterTables, 1)
	td := cs.Databases[0].AlterTables[0]
	require.NotNil(t, td.EngineChange)
	assert.Equal(t, []string{"client_agent"}, td.DropColumns,
		"column presence differences surface when the engine changes")
}
