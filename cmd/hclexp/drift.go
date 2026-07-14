package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// zkUUIDRe matches a table UUID embedded in a ReplicatedMergeTree zoo_path.
// ClickHouse expands the {uuid} macro to the table's literal UUID at CREATE
// time (while keeping {shard}/{replica} as macros), so the same logical
// table gets a different path on every shard — noise for cross-node drift.
var zkUUIDRe = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// driftNode is one per-node dump loaded for drift analysis: its identity
// (from the node{} block and/or the filename) plus its resolved schema.
type driftNode struct {
	Name    string
	File    string
	Macros  map[string]string
	Role    string // deployment role parsed from the filename suffix
	Shard   string
	Replica string
	Schema  *hclload.Schema
}

// nodeNameRe extracts shard, replica, and the optional deployment-role
// suffix from a node name like prod-eu-fra-ch-10a-ingestion-events
// (shard 10, replica a, role ingestion-events) or prod-eu-fra-ch-1f
// (shard 1, replica f, no role suffix).
var nodeNameRe = regexp.MustCompile(`^[a-z]+-[a-z]+-[a-z]+-ch-([0-9]+)([a-z])(?:-(.+))?$`)

// runDrift compares the per-node HCL dumps in a directory, grouping nodes
// that are expected to share a schema and reporting any drift within each
// group. It exits non-zero when any drift is found.
func runDrift(args []string) {
	fs := flag.NewFlagSet("hclexp drift", flag.ExitOnError)
	dirFlag := fs.String("dir", "", "directory of per-node .hcl dumps to compare")
	globFlag := fs.String("glob", "*", "comma-separated filename globs selecting dumps within -dir; a file matching any pattern is included, e.g. '*[fg].hcl,*-offline.hcl' for all data nodes")
	groupByFlag := fs.String("group-by", "hostClusterRole", "comma-separated keys to group nodes by: macro names, or the pseudo-keys role/shard/replica")
	zkFlag := fs.String("zk-paths", "mask-uuid", "treat ReplicatedMergeTree zoo_path before diffing: keep | mask-uuid | ignore")
	details := fs.Bool("details", false, "print the full change set of each drifting node against its group reference")
	excludeFlag := fs.String("exclude", "", "HCL exclude config: objects matching its patterns/object_types are dropped from every node before comparing")
	formatFlag := fs.String("format", "text", "output format: text (default) or json")
	_ = fs.Parse(args)

	if *dirFlag == "" {
		fmt.Fprintln(os.Stderr, "drift: -dir is required")
		os.Exit(2)
	}
	switch *zkFlag {
	case "keep", "mask-uuid", "ignore":
	default:
		fmt.Fprintf(os.Stderr, "drift: invalid -zk-paths %q (want keep|mask-uuid|ignore)\n", *zkFlag)
		os.Exit(2)
	}
	switch *formatFlag {
	case "text", "json":
	default:
		fmt.Fprintf(os.Stderr, "drift: invalid -format %q (want text|json)\n", *formatFlag)
		os.Exit(2)
	}

	nodes, err := loadDriftNodes(*dirFlag, *globFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drift: %v\n", err)
		os.Exit(1)
	}
	for i := range nodes {
		normalizeZKPaths(nodes[i].Schema, *zkFlag)
	}
	if m := loadExcludeFlag(*excludeFlag); m != nil {
		for i := range nodes {
			hclload.FilterSchema(nodes[i].Schema, m)
		}
	}
	if len(nodes) == 0 {
		fmt.Fprintf(os.Stderr, "drift: no .hcl files in %s match %q\n", *dirFlag, *globFlag)
		os.Exit(1)
	}

	keys := splitList(*groupByFlag)
	doc := buildDriftDoc(nodes, keys)

	if *formatFlag == "json" {
		out, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "drift: render JSON: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	} else {
		renderDriftText(os.Stdout, doc, *details)
	}
	if doc.Summary.DriftingNodes > 0 {
		os.Exit(1)
	}
}

// buildDriftDoc groups nodes and diffs each group against its
// lexically-first reference, returning the full drift document. Groups with
// a single node get no drifters (nothing to compare against). Groups and
// Drifters are non-nil so JSON emits [] (not null) — same contract as
// DiffJSON.Objects.
func buildDriftDoc(nodes []driftNode, keys []string) hclload.DriftJSON {
	groups, order := groupNodes(nodes, keys)
	doc := hclload.DriftJSON{
		Groups:  []hclload.DriftGroup{},
		Summary: hclload.DriftRunSummary{Nodes: len(nodes), Groups: len(order)},
	}
	for _, key := range order {
		members := groups[key]
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
		ref := members[0]
		g := hclload.DriftGroup{Key: key, Reference: ref.Name, Nodes: len(members),
			Drifters: []hclload.DriftNode{}}
		for _, m := range members[1:] {
			cs := hclload.Diff(ref.Schema, m.Schema)
			if cs.IsEmpty() {
				continue
			}
			gen := hclload.GenerateSQL(cs)
			objs := hclload.BuildObjectComparisons(cs, gen, ref.Schema, m.Schema)
			g.Drifters = append(g.Drifters, hclload.DriftNode{
				Node: m.Name, File: m.File, Macros: m.Macros,
				Objects: objs, Summary: hclload.SummarizeComparisons(objs),
			})
		}
		if len(g.Drifters) > 0 {
			doc.Summary.GroupsWithDrift++
			doc.Summary.DriftingNodes += len(g.Drifters)
		}
		doc.Groups = append(doc.Groups, g)
	}
	return doc
}

// renderDriftText prints the classic prose report from the drift document.
func renderDriftText(w io.Writer, doc hclload.DriftJSON, details bool) {
	for _, g := range doc.Groups {
		if g.Nodes < 2 {
			fmt.Fprintf(w, "group %q — 1 node (no peers to compare): %s\n", g.Key, g.Reference)
			continue
		}
		fmt.Fprintf(w, "group %q — %d nodes, reference %s", g.Key, g.Nodes, g.Reference)
		if len(g.Drifters) == 0 {
			fmt.Fprintf(w, " — OK (all identical)\n")
			continue
		}
		fmt.Fprintf(w, " — %d drifting\n", len(g.Drifters))
		for _, d := range g.Drifters {
			fmt.Fprintf(w, "  ✗ %s: %s\n", d.Node, d.Summary.OneLiner())
			if details {
				hclload.RenderObjectComparisons(prefixWriter(w, "      "), d.Objects)
			}
		}
	}
	fmt.Fprintf(w, "\nsummary: %d nodes, %d groups, %d groups with drift, %d drifting nodes\n",
		doc.Summary.Nodes, doc.Summary.Groups, doc.Summary.GroupsWithDrift, doc.Summary.DriftingNodes)
}

// loadDriftNodes parses and resolves every .hcl file in dir whose base name
// matches any of the comma-separated globs into a driftNode, taking identity
// from the file's node{} block when present and otherwise from the filename.
// An empty glob matches every .hcl file.
func loadDriftNodes(dir, glob string) ([]driftNode, error) {
	globs := splitList(glob)
	if len(globs) == 0 {
		globs = []string{"*"}
	}
	// Fail fast on an invalid pattern rather than silently matching nothing.
	for _, g := range globs {
		if _, err := filepath.Match(g, ""); err != nil {
			return nil, fmt.Errorf("invalid -glob %q: %w", g, err)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %q: %w", dir, err)
	}
	var nodes []driftNode
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".hcl" {
			continue
		}
		if !matchesAny(globs, e.Name()) {
			continue
		}
		path := filepath.Join(dir, e.Name())
		schema, err := loadSide(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", path, err)
		}

		n := driftNode{
			File:   path,
			Name:   strings.TrimSuffix(e.Name(), ".hcl"),
			Schema: schema,
		}
		if len(schema.Nodes) > 0 {
			if schema.Nodes[0].Name != "" {
				n.Name = schema.Nodes[0].Name
			}
			n.Macros = schema.Nodes[0].Macros
		}
		n.Shard, n.Replica, n.Role = parseNodeIdentity(n.Name)
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// normalizeZKPaths rewrites every table's ReplicatedMergeTree zoo_path in
// schema according to mode, so per-node path noise doesn't register as
// drift:
//
//   - keep      — leave paths untouched (raw comparison)
//   - mask-uuid — replace the literal table UUID with the {uuid} macro,
//     so the same table on different shards compares equal while genuine
//     path differences (e.g. a different database) still drift
//   - ignore    — blank zoo_path and replica_name entirely
func normalizeZKPaths(schema *hclload.Schema, mode string) {
	if schema == nil || mode == "keep" {
		return
	}
	for di := range schema.Databases {
		tables := schema.Databases[di].Tables
		for ti := range tables {
			t := &tables[ti]
			if t.Engine == nil || t.Engine.Decoded == nil {
				continue
			}
			t.Engine.Decoded = normalizeEngineZK(t.Engine.Decoded, mode)
		}
	}
}

// normalizeEngineZK returns dec with its ZooPath (and, for ignore mode,
// ReplicaName) rewritten per mode. Engines without a ZooPath field are
// returned unchanged. It works across all ReplicatedMergeTree variants via
// reflection on the shared field names.
func normalizeEngineZK(dec hclload.Engine, mode string) hclload.Engine {
	v := reflect.ValueOf(dec)
	if v.Kind() != reflect.Struct {
		return dec
	}
	cp := reflect.New(v.Type()).Elem()
	cp.Set(v)

	zp := cp.FieldByName("ZooPath")
	if !zp.IsValid() || zp.Kind() != reflect.String || !zp.CanSet() {
		return dec
	}
	switch mode {
	case "mask-uuid":
		zp.SetString(zkUUIDRe.ReplaceAllString(zp.String(), "{uuid}"))
	case "ignore":
		zp.SetString("")
		if rn := cp.FieldByName("ReplicaName"); rn.IsValid() && rn.Kind() == reflect.String && rn.CanSet() {
			rn.SetString("")
		}
	}
	return cp.Interface().(hclload.Engine)
}

// matchesAny reports whether name matches at least one of the globs.
func matchesAny(globs []string, name string) bool {
	for _, g := range globs {
		if ok, _ := filepath.Match(g, name); ok {
			return true
		}
	}
	return false
}

// parseNodeIdentity extracts shard, replica, and deployment role from a
// node name following the prod-<region>-<az>-ch-<shard><replica>[-role]
// convention. Unmatched names yield empty strings.
func parseNodeIdentity(name string) (shard, replica, role string) {
	m := nodeNameRe.FindStringSubmatch(name)
	if m == nil {
		return "", "", ""
	}
	return m[1], m[2], m[3]
}

// groupNodes buckets nodes by the concatenation of the given keys. Each key
// is resolved against the node's macros first, then the pseudo-keys
// role/shard/replica. It returns the buckets and a deterministic key order.
func groupNodes(nodes []driftNode, keys []string) (map[string][]driftNode, []string) {
	groups := map[string][]driftNode{}
	var order []string
	for _, n := range nodes {
		key := groupKey(n, keys)
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], n)
	}
	sort.Strings(order)
	return groups, order
}

// groupKey computes a node's group key from the requested keys.
func groupKey(n driftNode, keys []string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		var v string
		if mv, ok := n.Macros[k]; ok {
			v = mv
		} else {
			switch k {
			case "role":
				v = n.Role
			case "shard":
				v = n.Shard
			case "replica":
				v = n.Replica
			}
		}
		if v == "" {
			v = "(none)"
		}
		parts = append(parts, v)
	}
	return strings.Join(parts, "/")
}

// prefixWriter returns an io.Writer that prepends prefix to every line.
func prefixWriter(w io.Writer, prefix string) *linePrefixer {
	return &linePrefixer{w: w, prefix: prefix, atLineStart: true}
}

type linePrefixer struct {
	w           io.Writer
	prefix      string
	atLineStart bool
}

func (lp *linePrefixer) Write(p []byte) (int, error) {
	for _, b := range p {
		if lp.atLineStart {
			if _, err := io.WriteString(lp.w, lp.prefix); err != nil {
				return 0, err
			}
			lp.atLineStart = false
		}
		if _, err := lp.w.Write([]byte{b}); err != nil {
			return 0, err
		}
		if b == '\n' {
			lp.atLineStart = true
		}
	}
	return len(p), nil
}
