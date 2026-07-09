package main

import (
	"net/http"
	"reflect"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sectionsSchema exercises every HTML section builder in one database: a table
// with indexes, projections, and both constraint kinds; an MV with an explicit
// column list; a view and a dictionary with all optional props set; and a raw
// escape-hatch block. The MV reads from and writes to the declared table, so
// the schema validates clean and grows no Kafka-rooted flows.
func sectionsSchema() *hclload.Schema {
	return &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "analytics",
		Tables: []hclload.TableSpec{{
			Name:       "events",
			PrimaryKey: []string{"id"},
			OrderBy:    []string{"id", "ts"},
			Columns: []hclload.ColumnSpec{
				{Name: "id", Type: "UInt64"},
				{Name: "ts", Type: "DateTime"},
				{Name: "props", Type: "String"},
			},
			Indexes: []hclload.IndexSpec{
				{Name: "idx_props", Expr: "props", Type: "bloom_filter", Granularity: 4},
				{Name: "idx_ts", Expr: "ts", Type: "minmax"},
			},
			Projections: []hclload.ProjectionSpec{{Name: "proj_by_ts", Query: "SELECT ts, id ORDER BY ts"}},
			Constraints: []hclload.ConstraintSpec{
				{Name: "c_positive", Check: ptrStr("id > 0")},
				{Name: "c_recent", Assume: ptrStr("ts > toDateTime(0)")},
			},
			Engine: &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
		}},
		MaterializedViews: []hclload.MaterializedViewSpec{{
			Name:    "events_rollup",
			ToTable: "analytics.events",
			Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
			Query:   "SELECT id FROM analytics.events",
		}},
		Views: []hclload.ViewSpec{{
			Name:          "events_view",
			Query:         "SELECT id, ts FROM analytics.events",
			ColumnAliases: []string{"event_id", "event_ts"},
			SQLSecurity:   ptrStr("definer"),
			Definer:       ptrStr("admin_user"),
			Cluster:       ptrStr("main"),
			Comment:       ptrStr("recent events"),
		}},
		Dictionaries: []hclload.DictionarySpec{{
			Name:       "user_dict",
			PrimaryKey: []string{"user_id"},
			Attributes: []hclload.DictionaryAttribute{
				{Name: "user_id", Type: "UInt64"},
				{Name: "email", Type: "String", Default: ptrStr("''"), Expression: ptrStr("lower(raw_email)")},
			},
			Source:   &hclload.DictionarySourceSpec{Kind: "clickhouse"},
			Layout:   &hclload.DictionaryLayoutSpec{Kind: "hashed"},
			Settings: map[string]string{"max_threads": "2"},
			Cluster:  ptrStr("main"),
			Comment:  ptrStr("users by id"),
		}},
		Raws: []hclload.RawSpec{{
			Kind: "table",
			Name: "legacy_raw",
			SQL:  "CREATE TABLE analytics.legacy_raw (id UInt64) ENGINE = TinyLog\n",
		}},
	}}}
}

func TestWebSections_FindHelpers(t *testing.T) {
	db := &sectionsSchema().Databases[0]

	v := findView(db, "events_view")
	require.NotNil(t, v)
	assert.Equal(t, "events_view", v.Name)
	assert.Nil(t, findView(db, "missing"))

	d := findDictionary(db, "user_dict")
	require.NotNil(t, d)
	assert.Equal(t, "user_dict", d.Name)
	assert.Nil(t, findDictionary(db, "missing"))

	r := findRaw(db, "legacy_raw")
	require.NotNil(t, r)
	assert.Equal(t, "legacy_raw", r.Name)
	assert.Nil(t, findRaw(db, "missing"))
}

func TestWebSections_KindLabel(t *testing.T) {
	cases := []struct{ kind, want string }{
		{hclload.KindTable, "Table"},
		{hclload.KindMaterializedView, "Materialized View"},
		{hclload.KindView, "View"},
		{hclload.KindDictionary, "Dictionary"},
		{hclload.KindRaw, "Raw"},
		{"custom_kind", "custom_kind"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, kindLabel(tc.kind))
	}
}

func TestWebSections_FormatValue(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"string", "abc", "abc"},
		{"int", 42, "42"},
		{"bool", true, "true"},
		{"float", 1.5, "1.5"},
		{"string slice", []string{"a", "b"}, "a, b"},
		{"pointer slice", []*string{ptrStr("x"), ptrStr("y")}, "x, y"},
		{"empty slice", []string{}, ""},
		{"map sorted", map[string]int{"b": 2, "a": 10}, "a=10, b=2"},
		{"pointer", ptrStr("deref"), "deref"},
		{"nil pointer", (*string)(nil), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatValue(reflect.ValueOf(tc.in)))
		})
	}
}

func TestWebSections_ViewProps(t *testing.T) {
	v := sectionsSchema().Databases[0].Views[0]
	assert.Equal(t, []kv{
		{"column_aliases", "event_id, event_ts"},
		{"sql_security", "definer"},
		{"definer", "admin_user"},
		{"cluster", "main"},
		{"comment", "recent events"},
	}, viewProps(v))

	assert.Empty(t, viewProps(hclload.ViewSpec{Name: "bare", Query: "SELECT 1"}))
}

func TestWebSections_DictProps(t *testing.T) {
	d := sectionsSchema().Databases[0].Dictionaries[0]
	assert.Equal(t, []kv{
		{"primary_key", "user_id"},
		{"source", "clickhouse"},
		{"layout", "hashed"},
		{"cluster", "main"},
		{"comment", "users by id"},
		{"max_threads", "2"},
	}, dictProps(d))

	assert.Empty(t, dictProps(hclload.DictionarySpec{Name: "bare"}))
}

func TestWebSections_DictAttributesSection(t *testing.T) {
	sec := dictAttributesSection(sectionsSchema().Databases[0].Dictionaries[0].Attributes)
	assert.Equal(t, tableSection{
		Title:   "Attributes",
		Headers: []string{"name", "type", "default", "expression"},
		Rows: [][]string{
			{"user_id", "UInt64", "", ""},
			{"email", "String", "''", "lower(raw_email)"},
		},
	}, sec)
}

func TestWebSections_IndexesSection(t *testing.T) {
	_, ok := indexesSection(nil)
	assert.False(t, ok)

	sec, ok := indexesSection(sectionsSchema().Databases[0].Tables[0].Indexes)
	require.True(t, ok)
	assert.Equal(t, tableSection{
		Title:   "Indexes",
		Headers: []string{"name", "expr", "type", "granularity"},
		Rows: [][]string{
			{"idx_props", "props", "bloom_filter", "4"},
			{"idx_ts", "ts", "minmax", ""},
		},
	}, sec)
}

func TestWebSections_ConstraintsSection(t *testing.T) {
	_, ok := constraintsSection(nil)
	assert.False(t, ok)

	sec, ok := constraintsSection(sectionsSchema().Databases[0].Tables[0].Constraints)
	require.True(t, ok)
	assert.Equal(t, tableSection{
		Title:   "Constraints",
		Headers: []string{"name", "kind", "expr"},
		Rows: [][]string{
			{"c_positive", "check", "id > 0"},
			{"c_recent", "assume", "ts > toDateTime(0)"},
		},
	}, sec)
}

func TestWebSections_ProjectionViews(t *testing.T) {
	assert.Nil(t, projectionViews(nil))

	views := projectionViews(sectionsSchema().Databases[0].Tables[0].Projections)
	assert.Equal(t, []projectionView{
		{Name: "proj_by_ts", Query: "SELECT ts, id ORDER BY ts"},
	}, views)
}

func TestWebSections_BuildHTMLViewNotFound(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)
	db := srv.findDatabase("analytics")
	require.NotNil(t, db)

	kinds := []string{
		hclload.KindTable,
		hclload.KindMaterializedView,
		hclload.KindView,
		hclload.KindDictionary,
		hclload.KindRaw,
		"unknown_kind",
	}
	for _, kind := range kinds {
		var data objectData
		assert.Falsef(t, srv.buildHTMLView(&data, db, kind, "missing"), "kind %s", kind)
	}
}

func TestWebSections_BuildHTMLViewRaw(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)
	db := srv.findDatabase("analytics")
	require.NotNil(t, db)

	var data objectData
	require.True(t, srv.buildHTMLView(&data, db, hclload.KindRaw, "legacy_raw"))
	assert.Equal(t, []kv{{"kind", "table"}}, data.Props)
	assert.Contains(t, data.RawSQL, "ENGINE = TinyLog")
}

func TestWebSections_TablePage(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/analytics/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "primary_key")
	assert.Contains(t, body, "Indexes")
	assert.Contains(t, body, "idx_props")
	assert.Contains(t, body, "bloom_filter")
	assert.Contains(t, body, "Constraints")
	assert.Contains(t, body, "c_positive")
	assert.Contains(t, body, "assume")
	assert.Contains(t, body, "Projections")
	assert.Contains(t, body, "proj_by_ts")
	assert.Contains(t, body, "SELECT ts, id ORDER BY ts")
}

func TestWebSections_MaterializedViewPage(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/analytics/materialized_view/events_rollup?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "to_table")
	assert.Contains(t, body, "analytics.events")
	assert.Contains(t, body, "Columns")
	assert.Contains(t, body, "SELECT id FROM analytics.events")
}

func TestWebSections_ViewPage(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/analytics/view/events_view?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "sql_security")
	assert.Contains(t, body, "admin_user")
	assert.Contains(t, body, "event_id, event_ts")
	assert.Contains(t, body, "SELECT id, ts FROM analytics.events")
}

func TestWebSections_DictionaryPage(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/analytics/dictionary/user_dict?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Dictionary")
	assert.Contains(t, body, "clickhouse")
	assert.Contains(t, body, "hashed")
	assert.Contains(t, body, "Attributes")
	assert.Contains(t, body, "user_id")
	assert.Contains(t, body, "lower(raw_email)")
}

func TestWebSections_RawPage(t *testing.T) {
	srv, err := newWebServer(sectionsSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/analytics/raw/legacy_raw?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Raw")
	assert.Contains(t, body, "legacy_raw")
	assert.Contains(t, body, "ENGINE = TinyLog")
}
