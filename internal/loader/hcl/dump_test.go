package hcl

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sortTables sorts each database's tables alphabetically so two resolved
// schemas can be compared without table-order skew.
func sortTables(dbs []DatabaseSpec) {
	for di := range dbs {
		sort.Slice(dbs[di].Tables, func(i, j int) bool {
			return dbs[di].Tables[i].Name < dbs[di].Tables[j].Name
		})
	}
}

// roundTrip parses, resolves, dumps, parses, resolves again. The before and
// after schemas (with engine bodies cleared) must compare equal.
func roundTrip(t *testing.T, file string) {
	t.Helper()

	before, err := ParseFile(file)
	require.NoError(t, err)
	require.NoError(t, Resolve(before))

	var buf bytes.Buffer
	require.NoError(t, Write(&buf, before))

	tmp := filepath.Join(t.TempDir(), "round_trip.hcl")
	require.NoError(t, os.WriteFile(tmp, buf.Bytes(), 0o644))

	after, err := ParseFile(tmp)
	require.NoError(t, err, "re-parse failed; dump output:\n%s", buf.String())
	require.NoError(t, Resolve(after))

	stripEngineBodies(before)
	stripEngineBodies(after)
	sortTables(before)
	sortTables(after)

	assert.Equal(t, before, after, "round-trip mismatch; dump output:\n%s", buf.String())
}

func TestWrite_RoundTrip_BasicResolve(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "resolve_basic.hcl"))
}

func TestWrite_RoundTrip_AllEngineKinds(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "engines_all_kinds.hcl"))
}

func TestWrite_RoundTrip_FullTable(t *testing.T) {
	roundTrip(t, filepath.Join("testdata", "dump_round_trip_full.hcl"))
}

func TestWrite_OutputIsStable(t *testing.T) {
	dbs, err := ParseFile(filepath.Join("testdata", "resolve_basic.hcl"))
	require.NoError(t, err)
	require.NoError(t, Resolve(dbs))

	var a, b bytes.Buffer
	require.NoError(t, Write(&a, dbs))
	require.NoError(t, Write(&b, dbs))
	assert.Equal(t, a.String(), b.String(), "dump output should be deterministic")
}
