package main

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// webTestSchema builds a small resolved schema: one table and one materialized
// view that writes into it, so dependency cross-links have something to point at.
func webTestSchema() *hclload.Schema {
	return &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "posthog",
		Tables: []hclload.TableSpec{{
			Name:    "events",
			OrderBy: []string{"id"},
			Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
			Engine:  &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
		}},
		MaterializedViews: []hclload.MaterializedViewSpec{{
			Name:    "events_mv",
			ToTable: "posthog.events",
			Query:   "SELECT id FROM posthog.src",
		}},
	}}}
}

func getBody(t *testing.T, srv *webServer, target string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.handleIndex)
	mux.HandleFunc("/flows", srv.handleFlows)
	mux.HandleFunc("/db/", srv.handleObject)
	staticSub, _ := fs.Sub(webFS, "web/static")
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))))
	mux.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// wideTable builds a table with n columns, for exercising the collapse toggle.
func wideTable(n int) hclload.TableSpec {
	cols := make([]hclload.ColumnSpec, n)
	for i := range cols {
		cols[i] = hclload.ColumnSpec{Name: "c" + strconv.Itoa(i), Type: "UInt64"}
	}
	return hclload.TableSpec{
		Name:    "wide",
		OrderBy: []string{"c0"},
		Columns: cols,
		Engine:  &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
	}
}

func TestWeb_Index(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "posthog")
	assert.Contains(t, body, "events")
	assert.Contains(t, body, "/db/posthog/table/events")
}

func TestWeb_TableViews(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)

	code, html := getBody(t, srv, "/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, html, "id")         // column name
	assert.Contains(t, html, "merge_tree") // engine prop

	code, ddl := getBody(t, srv, "/db/posthog/table/events?view=ddl")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, ddl, "CREATE TABLE posthog.events")

	code, hcl := getBody(t, srv, "/db/posthog/table/events?view=hcl")
	require.Equal(t, http.StatusOK, code)
	// HCL is HTML-escaped in the page, so the quotes appear as entities.
	assert.Contains(t, hcl, "table")
	assert.Contains(t, hcl, "events")
}

func TestWeb_MaterializedViewDependencyLink(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/posthog/materialized_view/events_mv?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Dependencies")
	// MV writes into posthog.events — a link back to that table object.
	assert.Contains(t, body, "/db/posthog/table/events")
	assert.Contains(t, body, "mv_dest")
}

func TestWeb_ColumnCollapseToggle(t *testing.T) {
	schema := &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name:   "posthog",
		Tables: []hclload.TableSpec{wideTable(15)},
	}}}
	srv, err := newWebServer(schema)
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/posthog/table/wide?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `class="collapse-toggle"`)
	assert.Contains(t, body, "grid collapsible")

	// app.js (which reads/writes localStorage) is served.
	code, js := getBody(t, srv, "/static/app.js")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, js, "localStorage")
}

func TestWeb_NoToggleForShortColumnList(t *testing.T) {
	// The default webTestSchema table has a single column — no toggle.
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "collapse-toggle")
	assert.NotContains(t, body, "grid collapsible")
}

// kafkaFlowSchema builds the canonical ingestion chain:
// Kafka table -> MV -> Distributed -> ReplicatedMergeTree.
func kafkaFlowSchema() *hclload.Schema {
	topic := "events"
	return &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "posthog",
		Tables: []hclload.TableSpec{
			{
				Name:    "kafka_events",
				Columns: []hclload.ColumnSpec{{Name: "team_id", Type: "UInt64"}},
				Engine:  &hclload.EngineSpec{Kind: "kafka", Decoded: hclload.EngineKafka{TopicList: &topic}},
			},
			{
				Name:    "sharded_events",
				Columns: []hclload.ColumnSpec{{Name: "team_id", Type: "UInt64"}},
				Engine: &hclload.EngineSpec{Kind: "distributed", Decoded: hclload.EngineDistributed{
					ClusterName: "posthog", RemoteDatabase: "posthog", RemoteTable: "events_local",
				}},
			},
			{
				Name:    "events_local",
				OrderBy: []string{"team_id"},
				Columns: []hclload.ColumnSpec{{Name: "team_id", Type: "UInt64"}},
				Engine:  &hclload.EngineSpec{Kind: "replicated_merge_tree", Decoded: hclload.EngineReplicatedMergeTree{ZooPath: "/c/{shard}/e", ReplicaName: "{replica}"}},
			},
		},
		MaterializedViews: []hclload.MaterializedViewSpec{{
			Name:    "events_mv",
			ToTable: "posthog.sharded_events",
			Query:   "SELECT team_id FROM posthog.kafka_events",
		}},
	}}}
}

func TestWeb_FlowsFullChain(t *testing.T) {
	srv, err := newWebServer(kafkaFlowSchema())
	require.NoError(t, err)

	require.Len(t, srv.flows, 1)
	stages := srv.flows[0].Stages
	require.Len(t, stages, 4)
	assert.Equal(t, []string{"kafka_events", "events_mv", "sharded_events", "events_local"},
		[]string{stages[0].Name, stages[1].Name, stages[2].Name, stages[3].Name})
	assert.Equal(t, []string{"", "reads", "writes to", "forwards to"},
		[]string{stages[0].EdgeLabel, stages[1].EdgeLabel, stages[2].EdgeLabel, stages[3].EdgeLabel})
	assert.Equal(t, "topic=events", stages[0].Detail)
	assert.Equal(t, "cluster=posthog", stages[2].Detail)

	code, body := getBody(t, srv, "/flows")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "/db/posthog/table/kafka_events")
	assert.Contains(t, body, "/db/posthog/materialized_view/events_mv")
	assert.Contains(t, body, "forwards to")
}

func TestWeb_FlowFanOut(t *testing.T) {
	// One Kafka source feeding two MVs → two distinct chains.
	schema := kafkaFlowSchema()
	db := &schema.Databases[0]
	db.MaterializedViews = append(db.MaterializedViews, hclload.MaterializedViewSpec{
		Name:    "events_mv2",
		ToTable: "posthog.events_local",
		Query:   "SELECT team_id FROM posthog.kafka_events",
	})
	srv, err := newWebServer(schema)
	require.NoError(t, err)
	assert.Len(t, srv.flows, 2)
}

func TestWeb_ObjectFlowBacklink(t *testing.T) {
	srv, err := newWebServer(kafkaFlowSchema())
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/posthog/table/kafka_events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "/flows#flow-1")
	assert.Contains(t, body, "Appears in a data flow")
}

func TestWeb_NoFlowBacklinkWhenNotInFlow(t *testing.T) {
	// webTestSchema's MV writes to a table that nothing reads further; the
	// standalone "events" table here is the MV target, so it is in a flow. Use a
	// schema with an isolated table instead.
	schema := &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "posthog",
		Tables: []hclload.TableSpec{{
			Name:    "lonely",
			OrderBy: []string{"id"},
			Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
			Engine:  &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
		}},
	}}}
	srv, err := newWebServer(schema)
	require.NoError(t, err)

	code, body := getBody(t, srv, "/db/posthog/table/lonely?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "Appears in a data flow")
}

func TestWeb_FlowProblems(t *testing.T) {
	// An MV that references a virtual column its source doesn't provide, plus a
	// second MV whose target table isn't declared.
	schema := &hclload.Schema{Databases: []hclload.DatabaseSpec{{
		Name: "posthog",
		Tables: []hclload.TableSpec{
			{
				Name:    "src",
				OrderBy: []string{"id"},
				Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
				Engine:  &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
			},
			{
				Name:    "dst",
				OrderBy: []string{"id"},
				Columns: []hclload.ColumnSpec{{Name: "id", Type: "UInt64"}},
				Engine:  &hclload.EngineSpec{Kind: "merge_tree", Decoded: hclload.EngineMergeTree{}},
			},
		},
		MaterializedViews: []hclload.MaterializedViewSpec{
			{
				Name:    "bad_columns_mv",
				ToTable: "posthog.dst",
				Query:   "SELECT id, _bogus FROM posthog.src",
			},
			{
				Name:    "missing_target_mv",
				ToTable: "posthog.nope",
				Query:   "SELECT id FROM posthog.src",
			},
		},
	}}}
	srv, err := newWebServer(schema)
	require.NoError(t, err)

	// The undefined-column problem is attributed to the MV.
	colProblems := srv.problems[indexKey("posthog", "bad_columns_mv")]
	require.NotEmpty(t, colProblems)
	assert.Equal(t, "undefined column", colProblems[0].Kind)

	// The missing-target problem is attributed to the other MV.
	tgtProblems := srv.problems[indexKey("posthog", "missing_target_mv")]
	require.NotEmpty(t, tgtProblems)
	assert.Equal(t, "missing target", tgtProblems[0].Kind)

	// At least one reconstructed flow is flagged as having problems.
	flagged := false
	for _, f := range srv.flows {
		if f.HasProblems {
			flagged = true
		}
	}
	assert.True(t, flagged, "a flow touching a broken MV should be flagged")

	code, body := getBody(t, srv, "/flows")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "this flow has problems")
	assert.Contains(t, body, "undefined column")

	// The MV's own page surfaces the problem too.
	code, page := getBody(t, srv, "/db/posthog/materialized_view/bad_columns_mv?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, page, "Problems")
	assert.Contains(t, page, "_bogus")
}

func TestWeb_NoProblemsWhenSchemaIsClean(t *testing.T) {
	srv, err := newWebServer(kafkaFlowSchema())
	require.NoError(t, err)
	for ref, p := range srv.problems {
		assert.Emptyf(t, p, "unexpected problems for %s", ref)
	}
	code, body := getBody(t, srv, "/flows")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "this flow has problems")
}

func TestWeb_NotFound(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)

	code, _ := getBody(t, srv, "/db/posthog/table/missing")
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = getBody(t, srv, "/db/nosuchdb/table/events")
	assert.Equal(t, http.StatusNotFound, code)
}
