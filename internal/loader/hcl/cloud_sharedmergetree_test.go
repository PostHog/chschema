package hcl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The DDL in these tests matches what a ClickHouse Cloud service returns when
// cloud_mode_engine = 2: every MergeTree-family table comes back as a Shared*
// engine carrying the same generated replication arguments.

const (
	cloudSharedArgs = "'" + cloudSharedZooPath + "', '" + cloudSharedReplicaName + "'"
)

func TestCollapseSharedEngine_Families(t *testing.T) {
	tests := []struct {
		name       string
		engine     string
		params     []string
		wantEngine string
		wantParams []string
	}{
		{
			name:       "plain",
			engine:     "SharedMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName},
			wantEngine: "MergeTree",
			wantParams: []string{},
		},
		{
			name:       "replacing without version",
			engine:     "SharedReplacingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName},
			wantEngine: "ReplacingMergeTree",
			wantParams: []string{},
		},
		{
			name:       "replacing with version",
			engine:     "SharedReplacingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName, "version"},
			wantEngine: "ReplacingMergeTree",
			wantParams: []string{"version"},
		},
		{
			name:       "replacing with version and is_deleted",
			engine:     "SharedReplacingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName, "ver", "is_deleted"},
			wantEngine: "ReplacingMergeTree",
			wantParams: []string{"ver", "is_deleted"},
		},
		{
			name:       "aggregating",
			engine:     "SharedAggregatingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName},
			wantEngine: "AggregatingMergeTree",
			wantParams: []string{},
		},
		{
			name:       "summing with columns",
			engine:     "SharedSummingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName, "total"},
			wantEngine: "SummingMergeTree",
			wantParams: []string{"total"},
		},
		{
			name:       "collapsing",
			engine:     "SharedCollapsingMergeTree",
			params:     []string{cloudSharedZooPath, cloudSharedReplicaName, "sign"},
			wantEngine: "CollapsingMergeTree",
			wantParams: []string{"sign"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotEngine, gotParams, err := collapseSharedEngine(tc.engine, tc.params)
			require.NoError(t, err)
			assert.Equal(t, tc.wantEngine, gotEngine)
			assert.Equal(t, tc.wantParams, gotParams)
		})
	}
}

// TestCollapseSharedEngine_PassesThroughNonShared: only Shared*MergeTree names
// are rewritten. Everything else reaches the engine switch untouched, so the
// collapse being on never changes how a non-Cloud engine is read.
func TestCollapseSharedEngine_PassesThroughNonShared(t *testing.T) {
	for _, name := range []string{"MergeTree", "ReplicatedMergeTree", "Distributed", "Kafka", "SharedSet"} {
		params := []string{"a", "b"}
		gotName, gotParams, err := collapseSharedEngine(name, params)
		require.NoError(t, err)
		assert.Equal(t, name, gotName)
		assert.Equal(t, params, gotParams)
	}
}

// TestCollapseSharedEngine_UnexpectedArgsAbort: only the arguments Cloud
// generates are known to carry no information. Anything else must abort rather
// than be dropped — a dropped argument round-trips as a false "no drift"
// (#108, #109).
func TestCollapseSharedEngine_UnexpectedArgsAbort(t *testing.T) {
	tests := []struct {
		name   string
		params []string
	}{
		{"no arguments", nil},
		{"one argument", []string{cloudSharedZooPath}},
		{"custom zoo path", []string{"/clickhouse/tables/{shard}/events", cloudSharedReplicaName}},
		{"custom replica name", []string{cloudSharedZooPath, "replica_1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := collapseSharedEngine("SharedMergeTree", tc.params)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "SharedMergeTree")
			assert.Contains(t, err.Error(), cloudSharedZooPath)
		})
	}
}

// TestIntrospect_CloudSharedMergeTree_Collapsed walks real Cloud DDL through
// the whole introspection path with the collapse on, and back out through
// sqlgen. The emitted clause is the plain engine, which Cloud rewrites to the
// Shared form again on apply — so the round trip is stable and reports no
// drift.
func TestIntrospect_CloudSharedMergeTree_Collapsed(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{
		{
			name:   "events",
			sql:    "CREATE TABLE db.events (`id` UUID, `stage` String) ENGINE = SharedMergeTree(" + cloudSharedArgs + ") ORDER BY id SETTINGS index_granularity = 8192",
			engine: "SharedMergeTree",
		},
		{
			name:   "records",
			sql:    "CREATE TABLE db.records (`stage` String, `version` UInt64) ENGINE = SharedReplacingMergeTree(" + cloudSharedArgs + ", version) ORDER BY stage SETTINGS index_granularity = 8192",
			engine: "SharedReplacingMergeTree",
		},
		{
			name:   "totals",
			sql:    "CREATE TABLE db.totals (`k` String, `v` AggregateFunction(sum, UInt64)) ENGINE = SharedAggregatingMergeTree(" + cloudSharedArgs + ") ORDER BY k SETTINGS index_granularity = 8192",
			engine: "SharedAggregatingMergeTree",
		},
	}}

	db := &DatabaseSpec{Name: "db"}
	require.NoError(t, processIntrospectRowsOpt(db, "db", rows,
		IntrospectOptions{CollapseSharedMergeTree: true}))
	require.Len(t, db.Tables, 3)

	byName := map[string]TableSpec{}
	for _, tbl := range db.Tables {
		byName[tbl.Name] = tbl
	}

	assert.Equal(t, EngineMergeTree{}, byName["events"].Engine.Decoded)
	assert.Equal(t, "merge_tree", byName["events"].Engine.Kind)

	replacing, ok := byName["records"].Engine.Decoded.(EngineReplacingMergeTree)
	require.True(t, ok)
	require.NotNil(t, replacing.VersionColumn)
	assert.Equal(t, "version", *replacing.VersionColumn)
	assert.Nil(t, replacing.IsDeletedColumn)

	assert.Equal(t, EngineAggregatingMergeTree{}, byName["totals"].Engine.Decoded)

	// The generated replication arguments are gone, not smuggled into the
	// table settings.
	assert.Equal(t, map[string]string{"index_granularity": "8192"}, byName["events"].Settings)

	for name, want := range map[string]string{
		"events":  "MergeTree()",
		"records": "ReplacingMergeTree(version)",
		"totals":  "AggregatingMergeTree()",
	} {
		clause, extra := engineSQL(byName[name].Engine.Decoded)
		assert.Equal(t, want, clause, "sqlgen clause for %s", name)
		assert.Empty(t, extra)
	}
}

// TestIntrospect_SharedMergeTree_StrictWithoutServerSignal: without the server
// reporting that it rewrites DDL, a Shared* engine stays unsupported. The
// message names the setting that would change the answer.
func TestIntrospect_SharedMergeTree_StrictWithoutServerSignal(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{
		{
			name:   "events",
			sql:    "CREATE TABLE db.events (`id` UUID) ENGINE = SharedMergeTree(" + cloudSharedArgs + ") ORDER BY id",
			engine: "SharedMergeTree",
		},
	}}

	err := processIntrospectRowsOpt(&DatabaseSpec{Name: "db"}, "db", rows, IntrospectOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported engine: SharedMergeTree")
	assert.Contains(t, err.Error(), "cloud_mode_engine")
}

// TestIntrospect_SharedMergeTree_CustomPathAbortsEvenOnCloud: the collapse
// being on is not a licence to drop whatever arguments turn up.
func TestIntrospect_SharedMergeTree_CustomPathAbortsEvenOnCloud(t *testing.T) {
	rows := &fakeRows{rows: []fakeRow{
		{
			name:   "events",
			sql:    "CREATE TABLE db.events (`id` UUID) ENGINE = SharedMergeTree('/clickhouse/tables/{shard}/events', '{replica}') ORDER BY id",
			engine: "SharedMergeTree",
		},
	}}

	err := processIntrospectRowsOpt(&DatabaseSpec{Name: "db"}, "db", rows,
		IntrospectOptions{CollapseSharedMergeTree: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "will not drop arguments it did not expect")
}

// cloudSettingRows is a rowScanner over a single-column cloud_mode_engine
// result: zero rows when the server has no such setting.
type cloudSettingRows struct {
	values []string
	pos    int
	err    error
}

func (r *cloudSettingRows) Next() bool {
	r.pos++
	return r.pos <= len(r.values)
}

func (r *cloudSettingRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.values[r.pos-1]
	return nil
}

func (r *cloudSettingRows) Err() error { return r.err }

// TestClassifyCloudModeEngine covers both halves of the answer. "Does not
// rewrite" and "we have never heard of this" are different states: 0 and 1 are
// the server deliberately not rewriting, which is unremarkable, while an
// unrecognised value means ClickHouse has added a behaviour this build does
// not model and is worth warning about.
func TestClassifyCloudModeEngine(t *testing.T) {
	tests := []struct {
		value        string
		wantRewrites bool
		wantKnown    bool
	}{
		{"", false, true}, // setting absent: self-hosted, the common case
		{"0", false, true},
		{"1", false, true},
		{"2", true, true},
		{"3", true, true},
		{"4", true, true},
		{"5", false, false}, // a mode this build has never seen
		{"nonsense", false, false},
	}

	for _, tc := range tests {
		t.Run("value "+tc.value, func(t *testing.T) {
			rewrites, known := classifyCloudModeEngine(tc.value)
			assert.Equal(t, tc.wantRewrites, rewrites, "rewrites")
			assert.Equal(t, tc.wantKnown, known, "known")
		})
	}
}

func TestCloudEngineRewriteFromRows(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   bool
	}{
		{"rewrites to SharedMergeTree", []string{"2"}, true},
		{"SharedMergeTree except explicit remote disk", []string{"3"}, true},
		{"as 3, plus Alias instead of Distributed", []string{"4"}, true},
		// 0 and 1 read the same on the way in but not on the way out: the
		// plain engine sqlgen emits would stay a MergeTree (0) or become a
		// ReplicatedMergeTree (1), so collapsing would change the engine.
		{"allows everything", []string{"0"}, false},
		{"rewrites to ReplicatedMergeTree", []string{"1"}, false},
		// An unrecognised mode stops rather than being assumed to rewrite the
		// same way as the modes above it.
		{"mode this build has never seen", []string{"5"}, false},
		{"setting absent (self-hosted)", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cloudEngineRewriteFromRows(&cloudSettingRows{values: tc.values})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCloudEngineRewriteFromRows_PropagatesError(t *testing.T) {
	_, err := cloudEngineRewriteFromRows(&cloudSettingRows{err: errors.New("connection reset")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
}
