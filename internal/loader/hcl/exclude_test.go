package hcl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExcludeMatcher_Matches(t *testing.T) {
	m := NewExcludeMatcher("tmp_*", "_tmp_replace_*", "*_backup", "posthog.*_staging")

	// bare-name globs
	assert.True(t, m.Matches("posthog", "tmp_dag_team_2_prop_rm_019d7706"))
	assert.True(t, m.Matches("default", "_tmp_replace_5c4a29_abc"))
	assert.True(t, m.Matches("posthog", "sharded_events_backup"))
	// qualified glob
	assert.True(t, m.Matches("posthog", "web_stats_staging"))
	assert.False(t, m.Matches("other", "web_stats_staging"), "qualified pattern is db-scoped")
	// real objects are kept
	assert.False(t, m.Matches("posthog", "events"))
	assert.False(t, m.Matches("posthog", "sharded_query_log_archive"))

	// Match returns the pattern that matched (for logging).
	pat, ok := m.Match("posthog", "tmp_person_0007")
	assert.True(t, ok)
	assert.Equal(t, "tmp_*", pat)
	pat, ok = m.Match("posthog", "web_stats_staging")
	assert.True(t, ok)
	assert.Equal(t, "posthog.*_staging", pat)
	_, ok = m.Match("posthog", "events")
	assert.False(t, ok)

	// nil matcher excludes nothing
	var nilM *ExcludeMatcher
	assert.False(t, nilM.Matches("posthog", "tmp_anything"))
	_, ok = nilM.Match("posthog", "tmp_anything")
	assert.False(t, ok)
	assert.True(t, nilM.Empty())
}

func TestLoadExcludeConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exclude.hcl")
	require.NoError(t, os.WriteFile(path, []byte(`exclude {
  patterns = [
    "tmp_*",
    "*_backup",
    "*_backup_test",
  ]
}
`), 0o600))

	m, err := LoadExcludeConfig(path)
	require.NoError(t, err)
	assert.False(t, m.Empty())
	assert.True(t, m.Matches("posthog", "tmp_person_0007"))
	assert.True(t, m.Matches("posthog", "sharded_events_backup_test"))
	assert.False(t, m.Matches("posthog", "events"))
}

// TestLoadExcludeConfig_Example guards the committed examples/exclude.hcl: it
// must load and match the transient objects observed in real dumps while
// keeping the genuine schema objects.
func TestLoadExcludeConfig_Example(t *testing.T) {
	m, err := LoadExcludeConfig(filepath.Join("..", "..", "..", "examples", "exclude.hcl"))
	require.NoError(t, err)

	for _, name := range []string{
		"_tmp_replace_5c4a29cec09b9d73_cjofwgpr5ab97f08",
		"tmp_dag_team_12377_prop_rm_019ed48a",
		"tmp_person_0007",
		"infi_clickhouse_orm_migrations_tmp",
		"sharded_events_backup_test",
		"person_distinct_id_backup",
		"data_temp",
		"sharded_query_log_archive_temp_backfill",
		"web_bounces_hourly_staging",
	} {
		assert.True(t, m.Matches("posthog", name), "should exclude transient object %q", name)
	}
	for _, name := range []string{"events", "sharded_query_log_archive", "query_log_archive", "person"} {
		assert.False(t, m.Matches("posthog", name), "should keep real object %q", name)
	}
}

func TestLoadExcludeConfig_EmptyAndInvalid(t *testing.T) {
	dir := t.TempDir()

	// No exclude block -> matcher that excludes nothing.
	empty := filepath.Join(dir, "empty.hcl")
	require.NoError(t, os.WriteFile(empty, []byte("# nothing\n"), 0o600))
	m, err := LoadExcludeConfig(empty)
	require.NoError(t, err)
	assert.True(t, m.Empty())

	// Invalid glob -> error.
	bad := filepath.Join(dir, "bad.hcl")
	require.NoError(t, os.WriteFile(bad, []byte(`exclude { patterns = ["[bad"] }`), 0o600))
	_, err = LoadExcludeConfig(bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid exclude pattern")
}

// writeTempExclude writes an exclude config to a temp file and returns its path.
func writeTempExclude(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "exclude.hcl")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestFilterSchema(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{{
			Name: "posthog",
			Tables: []TableSpec{
				{Name: "events"},
				{Name: "tmp_migration"},
			},
			Views: []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
			Raws:  []RawSpec{{Kind: "table", Name: "legacy_backup", SQL: "CREATE ..."}},
		}},
		NamedCollections: []NamedCollectionSpec{{Name: "s3_creds"}},
	}

	m := NewExcludeMatcherWithTypes(
		[]string{KindNamedCollection},
		"tmp_*", "posthog.*_backup",
	)
	FilterSchema(s, m)

	want := &Schema{
		Databases: []DatabaseSpec{{
			Name:   "posthog",
			Tables: []TableSpec{{Name: "events"}},
			Views:  []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
			Raws:   []RawSpec{},
		}},
		NamedCollections: []NamedCollectionSpec{},
	}
	assert.Equal(t, want, s)
}

// SelectSchema is FilterSchema's inverse: only the matches survive, the
// database block stays even when emptied, and nodes are untouched.
func TestSelectSchema(t *testing.T) {
	s := &Schema{
		Databases: []DatabaseSpec{
			{
				Name: "posthog",
				Tables: []TableSpec{
					{Name: "events"},
					{Name: "person"},
				},
				Views:        []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
				Dictionaries: []DictionarySpec{{Name: "events_dict"}},
				Raws:         []RawSpec{{Kind: "dictionary", Name: "events_legacy", SQL: "CREATE ..."}},
			},
			{
				Name:   "other",
				Tables: []TableSpec{{Name: "person"}},
			},
		},
		NamedCollections: []NamedCollectionSpec{{Name: "events_creds"}, {Name: "s3_creds"}},
		Nodes:            []NodeSpec{{Name: "host-1"}},
	}

	SelectSchema(s, NewExcludeMatcher("events*", "other.person"))

	want := &Schema{
		Databases: []DatabaseSpec{
			{
				Name:         "posthog",
				Tables:       []TableSpec{{Name: "events"}},
				Views:        []ViewSpec{{Name: "events_view", Query: "SELECT 1"}},
				Dictionaries: []DictionarySpec{{Name: "events_dict"}},
				Raws:         []RawSpec{{Kind: "dictionary", Name: "events_legacy", SQL: "CREATE ..."}},
			},
			{
				Name:   "other",
				Tables: []TableSpec{{Name: "person"}},
			},
		},
		NamedCollections: []NamedCollectionSpec{{Name: "events_creds"}},
		Nodes:            []NodeSpec{{Name: "host-1"}},
	}
	assert.Equal(t, want, s)
}

// An empty (or nil) matcher would select nothing and erase the schema, so it
// is a no-op instead — a selector only acts when one was actually given.
func TestSelectSchema_EmptyMatcherIsNoop(t *testing.T) {
	s := &Schema{Databases: []DatabaseSpec{{Name: "d", Tables: []TableSpec{{Name: "t"}}}}}
	SelectSchema(s, nil)
	SelectSchema(s, NewExcludeMatcher())
	require.Len(t, s.Databases[0].Tables, 1)
}

func TestLoadExcludeConfig_ObjectTypes(t *testing.T) {
	path := writeTempExclude(t, `
exclude {
  patterns     = ["tmp_*"]
  object_types = ["named_collection"]
}`)
	m, err := LoadExcludeConfig(path)
	require.NoError(t, err)
	assert.True(t, m.MatchesObject(KindNamedCollection, "", "anything"))
	assert.True(t, m.MatchesObject(KindTable, "db", "tmp_x"))
	assert.False(t, m.MatchesObject(KindTable, "db", "events"))
	assert.False(t, m.Empty())

	_, err = LoadExcludeConfig(writeTempExclude(t, `
exclude {
  patterns     = []
  object_types = ["nonsense"]
}`))
	require.ErrorContains(t, err, "nonsense")
}
