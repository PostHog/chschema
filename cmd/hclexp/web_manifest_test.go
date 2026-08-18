package main

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tableLayer(table string) string {
	return `database "posthog" {
  table "` + table + `" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`
}

// manifestFixture lays out a manifest with two roles in one env, plus a second
// env for one role, and returns the root dir.
func manifestFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "layers/ops/s.hcl"), tableLayer("ops_table"))
	writeFileT(t, filepath.Join(root, "layers/data/s.hcl"), tableLayer("data_table"))
	writeFileT(t, filepath.Join(root, "manifest.hcl"), `role "ops" {
  env "prod-us" { layers = ["layers/ops"] }
  env "prod-eu" { layers = ["layers/ops"] }
}
role "data" {
  env "prod-us" { layers = ["layers/data"] }
}
`)
	return root
}

func getMulti(t *testing.T, ms *multiServer, target string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	ms.handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func getMultiWithCookie(t *testing.T, ms *multiServer, target string, cookie *http.Cookie) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	ms.handler().ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func TestComparisonSettingsReturn(t *testing.T) {
	ms := &multiServer{servers: map[string]*webServer{
		nodeBasePath("node-a"): nil,
	}}
	validCompare := objectCompareHref(nodeBasePath("node-a"), "node-b", "posthog", "table", "events")
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"review", "/object-diffs?status=different&q=EVENTS VIEW", "/object-diffs?q=EVENTS+VIEW&status=different"},
		{"mounted compare", validCompare, validCompare},
		{"absolute external", "https://attacker.example/phish", "/object-diffs"},
		{"scheme relative", "//attacker.example/phish", "/object-diffs"},
		{"backslash relative", `/\attacker.example/phish`, "/object-diffs"},
		{"double backslash", `/\\attacker.example/phish`, "/object-diffs"},
		{"encoded backslash", `/%5Cattacker.example/phish`, "/object-diffs"},
		{"triple slash", "///attacker.example/phish", "/object-diffs"},
		{"unmounted compare", "/n/missing/compare?peer=node-a", "/object-diffs"},
		{"unrelated local route", "/lookup?q=events", "/object-diffs"},
		{"control character", "/object-diffs\nLocation:https://attacker.example", "/object-diffs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ms.comparisonSettingsReturn(tc.raw))
		})
	}
}

func TestManifestCompositions(t *testing.T) {
	root := manifestFixture(t)
	mf := filepath.Join(root, "manifest.hcl")

	all, err := manifestCompositions(mf, "")
	require.NoError(t, err)
	require.Len(t, all, 3) // ops/prod-us, ops/prod-eu, data/prod-us

	us, err := manifestCompositions(mf, "prod-us")
	require.NoError(t, err)
	require.Len(t, us, 2)
	for _, c := range us {
		assert.Equal(t, "prod-us", c.Env)
	}

	_, err = manifestCompositions(mf, "nope")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no compositions for env "nope"`)
}

func TestWebManifest_BrowseSchemas(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	// Top-level list shows every env/role and links to each base path.
	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "prod-us")
	assert.Contains(t, body, "prod-eu")
	assert.Contains(t, body, `href="/s/prod-us/ops/"`)
	assert.Contains(t, body, `href="/s/prod-us/data/"`)
	assert.NotContains(t, body, "Review object differences", "the aggregate review is dump-only")
	code, _ = getMulti(t, ms, "/object-diffs")
	assert.Equal(t, http.StatusNotFound, code)

	// Each schema browses its own objects under its prefix.
	code, body = getMulti(t, ms, "/s/prod-us/ops/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "ops_table")
	assert.NotContains(t, body, "data_table")
	assert.Contains(t, body, "prod-us / ops", "nav shows the schema label")
	// Object links are prefixed with the schema base path.
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)

	code, body = getMulti(t, ms, "/s/prod-us/data/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "data_table")
	assert.NotContains(t, body, "ops_table")

	// Object detail page resolves through the prefix.
	code, body = getMulti(t, ms, "/s/prod-us/ops/db/posthog/table/ops_table?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "ops_table")

	// Unknown schema -> 404.
	code, _ = getMulti(t, ms, "/s/prod-us/nope/")
	assert.Equal(t, http.StatusNotFound, code)
}

func TestWebManifest_LookupAcrossAndWithinSchemas(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	code, body := getMulti(t, ms, "/lookup?q=ops_table")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)
	assert.Contains(t, body, `href="/s/prod-eu/ops/db/posthog/table/ops_table"`)
	assert.Contains(t, body, "prod-us / ops")
	assert.Contains(t, body, "prod-eu / ops")
	assert.NotContains(t, body, "prod-us / data")

	code, body = getMulti(t, ms, "/s/prod-us/ops/lookup?q=ops")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/s/prod-us/ops/db/posthog/table/ops_table"`)
	assert.NotContains(t, body, "prod-eu / ops")
}

func TestWebDump_BrowseAndLookupNodes(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), `node "node-a" {
  macros = { cluster = "cluster-a", hostClusterRole = "data" }
}
`+tableLayer("events_a"))
	writeFileT(t, filepath.Join(root, "node-b.hcl"), `node "node-b" {
  macros = { cluster = "cluster-b", hostClusterRole = "data" }
}
`+tableLayer("events_b"))

	ms, err := buildDumpMultiServer(root, "*", 0)
	require.NoError(t, err)
	require.Len(t, ms.servers, 2)

	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Dump nodes")
	assert.Contains(t, body, "cluster-a")
	assert.Contains(t, body, `href="/n/node-a/"`)
	assert.Contains(t, body, `href="/n/node-b/"`)

	code, body = getMulti(t, ms, "/n/node-a/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "events_a")
	assert.NotContains(t, body, "events_b")
	assert.Contains(t, body, "cluster-a / node-a")
	dbAnchor := databaseAnchor("posthog")
	assert.Contains(t, body, `id="`+dbAnchor+`"`)
	assert.Contains(t, body, `href="/n/node-a/#`+dbAnchor+`">posthog</a>`)
	assert.Contains(t, body, `href="/n/node-a/">node-a</a>`)

	code, body = getMulti(t, ms, "/lookup?q=events")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/n/node-a/db/posthog/table/events_a"`)
	assert.Contains(t, body, `href="/n/node-b/db/posthog/table/events_b"`)
	assert.Contains(t, body, "cluster-a / node-a")
	assert.Contains(t, body, "cluster-b / node-b")

	filtered, err := buildDumpMultiServer(root, "node-a.hcl", 0)
	require.NoError(t, err)
	require.Len(t, filtered.servers, 1, "-glob filters dump files before mounting")
}

func replicatedTableDump(node, cluster, uuid, columnType string) string {
	return `node "` + node + `" {
  macros = { cluster = "` + cluster + `" }
}
database "posthog" {
  table "events" {
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/` + uuid + `/events"
      replica_name = "{replica}"
    }
    order_by = ["id"]
    column "id" { type = "` + columnType + `" }
  }
}
`
}

func objectSummaryDump(node, cluster, uuid, eventType string, includePartial, includeClusterLocal bool) string {
	partial := ""
	if includePartial {
		partial = `
  table "partial" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
`
	}
	clusterLocal := ""
	if includeClusterLocal {
		clusterLocal = `
  table "cluster_local" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
`
	}
	return `node "` + node + `" {
  macros = { cluster = "` + cluster + `" }
}
database "posthog" {
  table "events" {
    engine "replicated_merge_tree" {
      zoo_path     = "/clickhouse/tables/` + uuid + `/events"
      replica_name = "{replica}"
    }
    order_by = ["id"]
    column "id" { type = "` + eventType + `" }
  }
  table "common" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
` + clusterLocal + partial + `
  view "events_view" {
    query = "SELECT id FROM posthog.events"
  }
}
`
}

func TestWebDump_ObjectDiffSummary(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), objectSummaryDump(
		"node-a", "cluster-a", "11111111-1111-1111-1111-111111111111", "UInt64", true, true))
	writeFileT(t, filepath.Join(root, "node-b.hcl"), objectSummaryDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "UInt64", false, true))
	writeFileT(t, filepath.Join(root, "node-c.hcl"), objectSummaryDump(
		"node-c", "cluster-b", "33333333-3333-3333-3333-333333333333", "String", false, false))

	ms, err := buildDumpMultiServer(root, "*", time.Second)
	require.NoError(t, err)

	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/object-diffs">Review object differences</a>`)

	code, body = getMulti(t, ms, "/object-diffs")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Objects across dumped nodes")
	assert.Contains(t, body, `<strong>5</strong><span>objects</span>`)
	assert.Contains(t, body, `<strong>1</strong><span>different</span>`)
	assert.Contains(t, body, `<strong>4</strong><span>uniform</span>`)
	assert.Contains(t, body, `<strong>1</strong><span>inconsistent presence</span>`)
	assert.Contains(t, body, `<strong>3</strong><span>dumped nodes</span>`)
	assert.Contains(t, body, `2 schema variants`)
	assert.Contains(t, body, `Schema 1 · baseline <span class="count">2</span>`,
		"UUID-only differences are grouped into one schema variant")
	assert.Contains(t, body, `Schema 2 <span class="count">1</span>`)
	assert.Contains(t, body, `incomplete in 1 cluster`)
	partialStart := strings.Index(body, `href="/n/node-a/db/posthog/table/partial">partial</a>`)
	require.NotEqual(t, -1, partialStart)
	partialEnd := strings.Index(body[partialStart:], "</tr>")
	require.NotEqual(t, -1, partialEnd)
	partialRow := body[partialStart : partialStart+partialEnd]
	assert.Contains(t, partialRow, `1 / 2 nodes in 1 cluster`)
	assert.Contains(t, partialRow, `href="/n/node-b/">cluster-a / node-b</a>`)
	assert.NotContains(t, partialRow, "cluster-b / node-c",
		"a cluster where the object is wholly absent must not contribute a missing node")

	clusterLocalStart := strings.Index(body, `href="/n/node-a/db/posthog/table/cluster_local">cluster_local</a>`)
	require.NotEqual(t, -1, clusterLocalStart)
	clusterLocalEnd := strings.Index(body[clusterLocalStart:], "</tr>")
	require.NotEqual(t, -1, clusterLocalEnd)
	clusterLocalRow := body[clusterLocalStart : clusterLocalStart+clusterLocalEnd]
	assert.Contains(t, clusterLocalRow, `2 / 2 nodes in 1 cluster`)
	assert.NotContains(t, clusterLocalRow, "incomplete",
		"whole-cluster absence is valid and must not be flagged")

	compareHref := objectCompareHref(nodeBasePath("node-a"), "node-c", "posthog", "table", "events")
	assert.Contains(t, body, strings.ReplaceAll(compareHref, "&", "&amp;"))
	assert.Contains(t, body, "Compare with schema 1")

	commonPos := strings.Index(body, `href="/n/node-a/db/posthog/table/common">common</a>`)
	clusterLocalPos := strings.Index(body, `href="/n/node-a/db/posthog/table/cluster_local">cluster_local</a>`)
	eventsPos := strings.Index(body, `href="/n/node-a/db/posthog/table/events">events</a>`)
	partialPos := strings.Index(body, `href="/n/node-a/db/posthog/table/partial">partial</a>`)
	viewPos := strings.Index(body, `href="/n/node-a/db/posthog/view/events_view">events_view</a>`)
	require.NotEqual(t, -1, commonPos)
	require.NotEqual(t, -1, clusterLocalPos)
	require.NotEqual(t, -1, eventsPos)
	require.NotEqual(t, -1, partialPos)
	require.NotEqual(t, -1, viewPos)
	assert.Less(t, clusterLocalPos, commonPos)
	assert.Less(t, commonPos, eventsPos)
	assert.Less(t, eventsPos, partialPos)
	assert.Less(t, partialPos, viewPos)

	code, body = getMulti(t, ms, "/object-diffs?status=different")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `>events</a>`)
	assert.NotContains(t, body, `>common</a>`)
	assert.NotContains(t, body, `>cluster_local</a>`)
	assert.NotContains(t, body, `>partial</a>`)
	assert.NotContains(t, body, `>events_view</a>`)
	assert.Contains(t, body, "Showing 1 of 5 objects")

	code, body = getMulti(t, ms, "/object-diffs?status=partial")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `>partial</a>`)
	assert.NotContains(t, body, `>events</a>`)
	assert.NotContains(t, body, `>cluster_local</a>`)

	code, body = getMulti(t, ms, "/object-diffs?q=EVENTS_VIEW")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `value="EVENTS_VIEW"`)
	assert.Contains(t, body, `>events_view</a>`)
	assert.NotContains(t, body, `>events</a>`)

	// Opening the aggregate review refreshes every mounted dump before grouping
	// variants, just like an individual object's Across dumped nodes section.
	nodeBFile := filepath.Join(root, "node-b.hcl")
	require.NoError(t, os.WriteFile(nodeBFile, []byte(objectSummaryDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "String", false, true)), 0o600))
	nodeB := ms.servers[nodeBasePath("node-b")]
	future := nodeB.lastCheck.Add(time.Hour)
	nodeB.now = func() time.Time { return future }
	require.NoError(t, os.Chtimes(nodeBFile, future, future))

	code, body = getMulti(t, ms, "/object-diffs?status=different")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `Schema 1 · baseline <span class="count">1</span>`)
	assert.Contains(t, body, `Schema 2 <span class="count">2</span>`,
		"the summary uses reloaded peer signatures without visiting that node first")
}

func TestWebDump_SemanticDiffAndSessionColumnOrder(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), `node "node-a" {
  macros = { cluster = "cluster-a" }
}
database "posthog" {
  table "events" {
    engine "merge_tree" {}
    order_by = ["deleted_at"]
    ttl = "deleted_at + toIntervalMonth(3) WHERE is_deleted = 1"
    column "deleted_at" { type = "DateTime" }
    column "is_deleted" { type = "UInt8" }
  }
}
`)
	writeFileT(t, filepath.Join(root, "node-b.hcl"), `node "node-b" {
  macros = { cluster = "cluster-b" }
}
database "posthog" {
  table "events" {
    engine "merge_tree" {}
    order_by = ["deleted_at"]
    ttl = "deleted_at + INTERVAL 3 MONTH WHERE is_deleted = 1"
    column "is_deleted" { type = "UInt8" }
    column "deleted_at" { type = "DateTime" }
  }
}
`)

	ms, err := buildDumpMultiServer(root, "*", 0)
	require.NoError(t, err)

	code, body := getMulti(t, ms, "/object-diffs")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `<strong>1</strong><span>different</span>`)
	assert.Contains(t, body, `2 schema variants`)
	assert.NotContains(t, body, `name="ignore_column_order" value="1" checked`)

	form := url.Values{
		"ignore_column_order": {"1"},
		"return":              {"/object-diffs"},
	}
	req := httptest.NewRequest(http.MethodPost, "/comparison-settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	ms.handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/object-diffs", rec.Header().Get("Location"))
	var sessionCookie *http.Cookie
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == columnOrderCookie {
			sessionCookie = cookie
		}
	}
	require.NotNil(t, sessionCookie)
	assert.Equal(t, "1", sessionCookie.Value)
	assert.Zero(t, sessionCookie.MaxAge, "the comparison preference is browser-session scoped")
	assert.False(t, sessionCookie.Secure, "direct HTTP must retain the non-sensitive preference")

	tlsReq := httptest.NewRequest(http.MethodPost, "/comparison-settings", strings.NewReader(form.Encode()))
	tlsReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tlsReq.TLS = &tls.ConnectionState{}
	tlsRec := httptest.NewRecorder()
	ms.handler().ServeHTTP(tlsRec, tlsReq)
	require.Equal(t, http.StatusSeeOther, tlsRec.Code)
	var tlsCookie *http.Cookie
	for _, cookie := range tlsRec.Result().Cookies() {
		if cookie.Name == columnOrderCookie {
			tlsCookie = cookie
		}
	}
	require.NotNil(t, tlsCookie)
	assert.True(t, tlsCookie.Secure, "the preference cookie is protected whenever the handler receives TLS")

	code, body = getMultiWithCookie(t, ms, "/object-diffs", sessionCookie)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `<strong>0</strong><span>different</span>`)
	assert.Contains(t, body, `<strong>1</strong><span>uniform</span>`)
	assert.Contains(t, body, `1 schema variant`)
	assert.Contains(t, body, `name="ignore_column_order" value="1" checked`)

	compareHref := objectCompareHref(nodeBasePath("node-a"), "node-b", "posthog", "table", "events")
	code, body = getMultiWithCookie(t, ms, compareHref, sessionCookie)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "No semantic differences under the current comparison settings.",
		"equivalent TTL syntax remains equal after the only real difference, column order, is ignored")
	assert.NotContains(t, body, "Patch to uniform")

	defaultIgnored, err := buildDumpMultiServerWithOptions(
		root, "*", 0, hclload.DiffOptions{IgnoreColumnOrder: true},
	)
	require.NoError(t, err)
	code, body = getMulti(t, defaultIgnored, "/object-diffs")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `<strong>0</strong><span>different</span>`)
	assert.Contains(t, body, `name="ignore_column_order" value="1" checked`,
		"the web startup option becomes the default for a new browser session")
}

func TestWebDump_ObjectPresenceSchemaMarkersAndDiff(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "node-a.hcl"), replicatedTableDump(
		"node-a", "cluster-a", "11111111-1111-1111-1111-111111111111", "UInt64"))
	writeFileT(t, filepath.Join(root, "node-b.hcl"), replicatedTableDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "UInt64"))
	writeFileT(t, filepath.Join(root, "node-c.hcl"), replicatedTableDump(
		"node-c", "cluster-b", "33333333-3333-3333-3333-333333333333", "String"))

	ms, err := buildDumpMultiServer(root, "*", time.Second)
	require.NoError(t, err)
	code, body := getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)

	assert.Contains(t, body, "Across dumped nodes")
	assert.Contains(t, body, "Present on 3 dumped nodes")
	assert.Contains(t, body, `href="/n/node-a/db/posthog/table/events"`)
	assert.Contains(t, body, `href="/n/node-b/db/posthog/table/events"`)
	assert.Contains(t, body, `href="/n/node-c/db/posthog/table/events"`)
	assert.Contains(t, body, `href="/n/node-b/">node-b</a>`)
	assert.Contains(t, body, `href="/n/node-b/#`+databaseAnchor("posthog")+`">posthog</a>`)
	assert.Contains(t, body, `href="/n/node-b/db/posthog/table/events">events</a>`)
	assert.Contains(t, body, `schema-marker schema-current">current`)
	assert.Contains(t, body, `schema-marker schema-same">same`,
		"different replicated table UUIDs are masked like drift")
	assert.Contains(t, body, "11111111-1111-1111-1111-111111111111",
		"comparison normalization must not mutate the schema shown on the object page")
	assert.Contains(t, body, `<a class="schema-marker schema-different"`,
		"a real column type change is marked different")
	assert.Contains(t, body, "1 differs")

	compareHref := objectCompareHref(nodeBasePath("node-a"), "node-c", "posthog", "table", "events")
	assert.Contains(t, body, strings.ReplaceAll(compareHref, "&", "&amp;"),
		"the different marker links to its canonical schema diff")
	code, diffBody := getMulti(t, ms, compareHref)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, diffBody, "Schema comparison: events")
	assert.Contains(t, diffBody, "Baseline")
	assert.Contains(t, diffBody, "cluster-a / node-a")
	assert.Contains(t, diffBody, "Compared node")
	assert.Contains(t, diffBody, "cluster-b / node-c")
	assert.Contains(t, diffBody, `class="diff-line diff-delete"`)
	assert.Contains(t, diffBody, `class="diff-line diff-add"`)
	assert.Contains(t, diffBody, "UInt64")
	assert.Contains(t, diffBody, "String")
	assert.Contains(t, diffBody, `name="return" value="`+strings.ReplaceAll(compareHref, "&", "&amp;")+`"`,
		"session settings return to the mounted node comparison, not the root mux")
	assert.NotContains(t, diffBody, "11111111-1111-1111-1111-111111111111",
		"the displayed diff uses the same UUID-masked HCL as the marker")

	swapHref := objectCompareHref(nodeBasePath("node-c"), "node-a", "posthog", "table", "events")
	assert.Contains(t, diffBody, strings.ReplaceAll(swapHref, "&", "&amp;"))
	assert.Contains(t, diffBody, "⇄ Swap sides")
	assert.Contains(t, diffBody, `data-schema-side="left"`)
	assert.Contains(t, diffBody, `Same schema as left side <span class="count">2</span>`)
	assert.Contains(t, diffBody, `Same schema as right side <span class="count">1</span>`)
	assert.Contains(t, diffBody, `href="/n/node-b/">node-b</a>`,
		"every node sharing the left schema is listed")
	leftNodeA := strings.Index(diffBody, `href="/n/node-a/">node-a</a>`)
	leftNodeB := strings.Index(diffBody, `href="/n/node-b/">node-b</a>`)
	require.NotEqual(t, -1, leftNodeA)
	require.NotEqual(t, -1, leftNodeB)
	assert.Less(t, leftNodeA, leftNodeB, "schema match nodes are sorted by cluster and node")

	code, swappedBody := getMulti(t, ms, swapHref)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, swappedBody, `<section class="compare-side baseline">
    <span class="compare-role">Baseline</span>
    <strong>cluster-b / node-c</strong>`)
	assert.Contains(t, swappedBody, `<section class="compare-side peer">
    <span class="compare-role">Compared node</span>
    <strong>cluster-a / node-a</strong>`)

	patchHref := objectPatchHref(nodeBasePath("node-a"), "node-c", "posthog", "table", "events")
	assert.Contains(t, diffBody, strings.ReplaceAll(patchHref, "&", "&amp;"))
	code, patchBody := getMulti(t, ms, strings.Split(patchHref, "#")[0])
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, patchBody, "MUST BE REVIEWED.")
	assert.Contains(t, patchBody, "Target: <strong>cluster-b / node-c</strong>")
	assert.Contains(t, patchBody, "Desired schema: <strong>cluster-a / node-a</strong>")
	assert.Contains(t, patchBody, "ALTER TABLE posthog.events\n  MODIFY COLUMN id UInt64;",
		"uniformization SQL is parser-beautified")
	assert.NotContains(t, patchBody, "33333333-3333-3333-3333-333333333333",
		"patch generation uses UUID-normalized schemas")

	swappedPatchHref := objectPatchHref(nodeBasePath("node-c"), "node-a", "posthog", "table", "events")
	code, swappedPatchBody := getMulti(t, ms, strings.Split(swappedPatchHref, "#")[0])
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, swappedPatchBody, "ALTER TABLE posthog.events\n  MODIFY COLUMN id String;",
		"swapping sides reverses the patch direction")

	code, sameBody := getMulti(t, ms, objectCompareHref(
		nodeBasePath("node-a"), "node-b", "posthog", "table", "events"))
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, sameBody, "No semantic differences under the current comparison settings.")
	assert.Contains(t, sameBody, `Same schema as both sides <span class="count">2</span>`)
	assert.NotContains(t, sameBody, "Patch to uniform")

	// Rows are always ordered by cluster and then node, even when the current
	// node sorts last.
	code, body = getMulti(t, ms, "/n/node-c/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	nodeAPos := strings.Index(body, `href="/n/node-a/">node-a</a>`)
	nodeBPos := strings.Index(body, `href="/n/node-b/">node-b</a>`)
	nodeCPos := strings.Index(body, `href="/n/node-c/">node-c</a>`)
	require.NotEqual(t, -1, nodeAPos)
	require.NotEqual(t, -1, nodeBPos)
	require.NotEqual(t, -1, nodeCPos)
	assert.Less(t, nodeAPos, nodeBPos)
	assert.Less(t, nodeBPos, nodeCPos)

	// Context remains visible on the source views, not only the overview.
	code, body = getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=hcl")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "Across dumped nodes")

	// Visiting one node refreshes every peer before comparing. A change in
	// node-b therefore updates node-a's marker without first opening node-b.
	nodeBFile := filepath.Join(root, "node-b.hcl")
	require.NoError(t, os.WriteFile(nodeBFile, []byte(replicatedTableDump(
		"node-b", "cluster-a", "22222222-2222-2222-2222-222222222222", "String")), 0o600))
	nodeB := ms.servers[nodeBasePath("node-b")]
	future := nodeB.lastCheck.Add(time.Hour)
	nodeB.now = func() time.Time { return future }
	require.NoError(t, os.Chtimes(nodeBFile, future, future))

	code, body = getMulti(t, ms, "/n/node-a/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "2 differ")
	assert.NotContains(t, body, `schema-marker schema-same">same`)
}

func TestWebDump_ObjectPatchSQLReportsUnsafeChanges(t *testing.T) {
	desired := webTestSchema()
	drifted := webTestSchema()
	drifted.Databases[0].Tables[0].OrderBy = []string{"tuple()"}

	sql, unsafe := objectPatchSQL(
		dumpObjectSnapshot{Schema: normalizedDumpSchema(drifted)},
		dumpObjectSnapshot{Schema: normalizedDumpSchema(desired)},
		"posthog", "table", "events",
		hclload.DiffOptions{},
	)
	assert.Contains(t, sql, "no automatic SQL was generated")
	assert.Contains(t, strings.Join(unsafe, "\n"), "ORDER BY change")
}

func TestWebDump_ObjectPatchSQLPreservesAddedColumnPosition(t *testing.T) {
	current := webTestSchema()
	desired := webTestSchema()
	current.Databases[0].Tables[0].Columns = []hclload.ColumnSpec{
		{Name: "a", Type: "UInt8"},
		{Name: "d", Type: "UInt8"},
	}
	desired.Databases[0].Tables[0].Columns = []hclload.ColumnSpec{
		{Name: "a", Type: "UInt8"},
		{Name: "b", Type: "UInt8"},
		{Name: "d", Type: "UInt8"},
	}

	sql, unsafe := objectPatchSQL(
		dumpObjectSnapshot{Schema: normalizedDumpSchema(current)},
		dumpObjectSnapshot{Schema: normalizedDumpSchema(desired)},
		"posthog", "table", "events",
		hclload.DiffOptions{},
	)
	require.Empty(t, unsafe)
	assert.Contains(t, sql, "ADD COLUMN b UInt8 AFTER a;")
}

func TestWebDump_ObjectPatchSQLCommentsEveryManualLine(t *testing.T) {
	current := webTestSchema()
	desired := webTestSchema()
	desired.Databases[0].Tables[0].Indexes = []hclload.IndexSpec{{
		Name: "idx_id", Expr: "id", Type: "minmax", Granularity: 1,
	}}

	sql, unsafe := objectPatchSQL(
		dumpObjectSnapshot{Schema: normalizedDumpSchema(current)},
		dumpObjectSnapshot{Schema: normalizedDumpSchema(desired)},
		"posthog", "table", "events",
		hclload.DiffOptions{},
	)
	require.Empty(t, unsafe)
	manualAt := strings.Index(sql, "-- MANUAL:")
	require.NotEqual(t, -1, manualAt)
	for _, line := range strings.Split(sql[manualAt:], "\n") {
		assert.True(t, strings.HasPrefix(line, "--"), "manual SQL line must remain commented: %q", line)
	}
}

func TestWebDump_ObjectPresenceCoversEveryBrowsableKind(t *testing.T) {
	baseline, err := newWebServer(sectionsSchema())
	require.NoError(t, err)
	baseline.basePath = nodeBasePath("node-a")
	baseline.label = "cluster-a / node-a"

	peerSchema := sectionsSchema()
	peerSchema.Databases[0].MaterializedViews[0].Query = "SELECT id + 1 AS id FROM analytics.events"
	peer, err := newWebServer(peerSchema)
	require.NoError(t, err)
	peer.basePath = nodeBasePath("node-b")
	peer.label = "cluster-b / node-b"

	ctx := newDumpWebContext(nil)
	require.NoError(t, baseline.attachDumpContext(ctx, dumpNodeIdentity{Cluster: "cluster-a", Node: "node-a"}))
	require.NoError(t, peer.attachDumpContext(ctx, dumpNodeIdentity{Cluster: "cluster-b", Node: "node-b"}))

	objects := []struct {
		kind string
		name string
	}{
		{"table", "events"},
		{"materialized_view", "events_rollup"},
		{"view", "events_view"},
		{"dictionary", "user_dict"},
		{"raw", "legacy_raw"},
	}
	review := ctx.objectReview(hclload.DiffOptions{})
	require.Equal(t, len(objects), review.TotalObjects)
	assert.Equal(t, 1, review.DifferentObjects)
	assert.Equal(t, len(objects)-1, review.UniformObjects)
	reviewed := make(map[string]string, len(review.Objects))
	for _, object := range review.Objects {
		reviewed[object.Kind] = object.Name
	}
	for _, object := range objects {
		assert.Equal(t, object.name, reviewed[object.kind], "aggregate review must include %s", object.kind)
	}

	for _, object := range objects {
		t.Run(object.kind, func(t *testing.T) {
			code, body := getBody(t, baseline, "/db/analytics/"+object.kind+"/"+object.name)
			require.Equal(t, http.StatusOK, code)
			assert.Contains(t, body, "Across dumped nodes")
			assert.Contains(t, body, "Present on 2 dumped nodes")
			assert.Contains(t, body, peer.basePath+"/db/analytics/"+object.kind+"/"+object.name)
		})
	}

	code, body := getBody(t, baseline, "/db/analytics/materialized_view/events_rollup")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `schema-marker schema-different`)
	assert.Contains(t, body, strings.ReplaceAll(objectCompareHref(
		baseline.basePath, "node-b", "analytics", "materialized_view", "events_rollup"), "&", "&amp;"))
}

func TestWeb_ObjectPresenceIsDumpOnly(t *testing.T) {
	srv, err := newWebServer(webTestSchema())
	require.NoError(t, err)
	code, body := getBody(t, srv, "/db/posthog/table/events")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "Across dumped nodes")
}

func TestWebDump_ResolvesAndLinksCrossClusterReferences(t *testing.T) {
	root := t.TempDir()
	writeFileT(t, filepath.Join(root, "proxy-node.hcl"), `node "proxy-node" {
  macros = { cluster = "query" }
}
database "posthog" {
  table "distributed_events" {
    engine "distributed" {
      cluster_name    = "storage_writable"
      remote_database = "posthog"
      remote_table    = "sharded_events"
    }
    column "id" { type = "UInt64" }
  }
  materialized_view "events_mv" {
    to_table = "posthog.distributed_events"
    query    = "SELECT id FROM posthog.source_events"
    column "id" { type = "UInt64" }
  }
}
`)
	writeFileT(t, filepath.Join(root, "storage-node.hcl"), `node "storage-node" {
  macros = { cluster = "storage" }
}
database "posthog" {
  table "sharded_events" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
  table "source_events" {
    engine "merge_tree" {}
    order_by = ["id"]
    column "id" { type = "UInt64" }
  }
}
`)

	ms, err := buildDumpMultiServer(root, "*", 0)
	require.NoError(t, err)

	// The _writable alias resolves through the loaded storage cluster for both
	// validation and navigation.
	code, body := getMulti(t, ms, "/n/proxy-node/db/posthog/table/distributed_events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "missing remote table")
	assert.Contains(t, body,
		`<th>remote_table</th><td><a href="/n/storage-node/db/posthog/table/sharded_events">sharded_events</a>`)
	assert.Contains(t, body,
		`href="/n/storage-node/db/posthog/table/sharded_events">posthog.sharded_events (distributed_remote)</a>`)

	// MV writes stay local, while a read source may resolve from any mapped
	// sibling cluster, matching topology validation.
	code, body = getMulti(t, ms, "/n/proxy-node/db/posthog/materialized_view/events_mv?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "missing source")
	assert.Contains(t, body,
		`<th>to_table</th><td><a href="/n/proxy-node/db/posthog/table/distributed_events">posthog.distributed_events</a>`)
	assert.Contains(t, body,
		`href="/n/storage-node/db/posthog/table/source_events">posthog.source_events (mv_source)</a>`)

	// The same loaded-cluster resolution carries through the complete flow:
	// remote source -> MV -> local Distributed -> remote storage table.
	code, body = getMulti(t, ms, "/n/proxy-node/flows")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `href="/n/storage-node/db/posthog/table/source_events">source_events</a>`)
	assert.Contains(t, body, `href="/n/storage-node/db/posthog/table/sharded_events">sharded_events</a>`)
	assert.NotContains(t, body, "not declared")
}

func TestWebDump_RejectsDuplicateNodeNames(t *testing.T) {
	root := t.TempDir()
	for _, file := range []string{"a.hcl", "b.hcl"} {
		writeFileT(t, filepath.Join(root, file), `node "same-node" {}
`+tableLayer(strings.TrimSuffix(file, ".hcl")))
	}
	_, err := buildDumpMultiServer(root, "*", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `duplicate node name "same-node"`)
}

// TestWebManifest_Example builds the committed examples/manifest fleet through
// the web-manifest path, guarding the example against rot.
func TestWebManifest_Example(t *testing.T) {
	root := filepath.Join("..", "..", "examples", "manifest")
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "")
	require.NoError(t, err)
	require.Len(t, comps, 6, "2 roles x 3 envs")

	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)
	require.Len(t, ms.servers, 6)

	// ops role composes the ops layer (system_metrics); the env patch adds region.
	code, body := getMulti(t, ms, "/s/prod-us/ops/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "system_metrics")
	code, body = getMulti(t, ms, "/s/prod-us/ops/db/posthog/table/events?view=html")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "region", "prod-us patch_table adds the region column")

	// data role omits the ops layer; dev adds debug_events.
	code, body = getMulti(t, ms, "/s/dev/data/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "debug_events")
	assert.NotContains(t, body, "system_metrics")
}

func TestWebManifest_EnvFilter(t *testing.T) {
	root := manifestFixture(t)
	comps, err := manifestCompositions(filepath.Join(root, "manifest.hcl"), "prod-eu")
	require.NoError(t, err)
	ms, err := buildMultiServer(comps, root, 0)
	require.NoError(t, err)

	// Only prod-eu/ops is mounted.
	code, body := getMulti(t, ms, "/")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "prod-eu")
	assert.NotContains(t, body, "prod-us")

	code, _ = getMulti(t, ms, "/s/prod-eu/ops/")
	assert.Equal(t, http.StatusOK, code)
	code, _ = getMulti(t, ms, "/s/prod-us/ops/")
	assert.Equal(t, http.StatusNotFound, code)
}
