package main

import (
	"flag"
	"fmt"
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
	globFlag := fs.String("glob", "*", "filename glob to select dumps within -dir, e.g. '*ingestion-small*'")
	groupByFlag := fs.String("group-by", "hostClusterRole", "comma-separated keys to group nodes by: macro names, or the pseudo-keys role/shard/replica")
	zkFlag := fs.String("zk-paths", "mask-uuid", "treat ReplicatedMergeTree zoo_path before diffing: keep | mask-uuid | ignore")
	details := fs.Bool("details", false, "print the full change set of each drifting node against its group reference")
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

	nodes, err := loadDriftNodes(*dirFlag, *globFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drift: %v\n", err)
		os.Exit(1)
	}
	for i := range nodes {
		normalizeZKPaths(nodes[i].Schema, *zkFlag)
	}
	if len(nodes) == 0 {
		fmt.Fprintf(os.Stderr, "drift: no .hcl files in %s match %q\n", *dirFlag, *globFlag)
		os.Exit(1)
	}

	keys := splitList(*groupByFlag)
	groups, order := groupNodes(nodes, keys)

	totalDrift := 0
	groupsWithDrift := 0
	for _, key := range order {
		members := groups[key]
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })

		if len(members) < 2 {
			fmt.Printf("group %q — 1 node (no peers to compare): %s\n", key, members[0].Name)
			continue
		}

		ref := members[0]
		var drifters []driftNode
		summaries := map[string]string{}
		changeSets := map[string]hclload.ChangeSet{}
		for _, m := range members[1:] {
			cs := hclload.Diff(ref.Schema, m.Schema)
			if cs.IsEmpty() {
				continue
			}
			drifters = append(drifters, m)
			summaries[m.Name] = summarizeChanges(cs)
			changeSets[m.Name] = cs
		}

		fmt.Printf("group %q — %d nodes, reference %s", key, len(members), ref.Name)
		if len(drifters) == 0 {
			fmt.Printf(" — OK (all identical)\n")
			continue
		}
		groupsWithDrift++
		fmt.Printf(" — %d drifting\n", len(drifters))
		for _, d := range drifters {
			totalDrift++
			fmt.Printf("  ✗ %s: %s\n", d.Name, summaries[d.Name])
			if *details {
				renderChangeSet(prefixWriter(os.Stdout, "      "), changeSets[d.Name])
			}
		}
	}

	fmt.Printf("\nsummary: %d nodes, %d groups, %d groups with drift, %d drifting nodes\n",
		len(nodes), len(order), groupsWithDrift, totalDrift)
	if totalDrift > 0 {
		os.Exit(1)
	}
}

// loadDriftNodes parses and resolves every .hcl file in dir whose base name
// matches glob into a driftNode, taking identity from the file's node{}
// block when present and otherwise from the filename. An empty glob matches
// every .hcl file.
func loadDriftNodes(dir, glob string) ([]driftNode, error) {
	if glob == "" {
		glob = "*"
	}
	// Fail fast on an invalid pattern rather than silently matching nothing.
	if _, err := filepath.Match(glob, ""); err != nil {
		return nil, fmt.Errorf("invalid -glob %q: %w", glob, err)
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
		if ok, _ := filepath.Match(glob, e.Name()); !ok {
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

// summarizeChanges renders a one-line count of a ChangeSet.
func summarizeChanges(cs hclload.ChangeSet) string {
	var addT, dropT, altT, addMV, dropMV, altMV, addD, dropD, altD, addV, dropV, altV int
	for _, dc := range cs.Databases {
		addT += len(dc.AddTables)
		dropT += len(dc.DropTables)
		altT += len(dc.AlterTables)
		addMV += len(dc.AddMaterializedViews)
		dropMV += len(dc.DropMaterializedViews)
		altMV += len(dc.AlterMaterializedViews)
		addD += len(dc.AddDictionaries)
		dropD += len(dc.DropDictionaries)
		altD += len(dc.AlterDictionaries)
		addV += len(dc.AddViews)
		dropV += len(dc.DropViews)
		altV += len(dc.AlterViews)
	}
	var parts []string
	add := func(label string, plus, minus, alt int) {
		if plus > 0 {
			parts = append(parts, fmt.Sprintf("+%d %s", plus, label))
		}
		if minus > 0 {
			parts = append(parts, fmt.Sprintf("-%d %s", minus, label))
		}
		if alt > 0 {
			parts = append(parts, fmt.Sprintf("~%d %s", alt, label))
		}
	}
	add("table", addT, dropT, altT)
	add("mv", addMV, dropMV, altMV)
	add("dict", addD, dropD, altD)
	add("view", addV, dropV, altV)
	if len(parts) == 0 {
		return "changed"
	}
	return strings.Join(parts, ", ")
}

// prefixWriter returns an io.Writer that prepends prefix to every line.
func prefixWriter(w *os.File, prefix string) *linePrefixer {
	return &linePrefixer{w: w, prefix: prefix, atLineStart: true}
}

type linePrefixer struct {
	w           *os.File
	prefix      string
	atLineStart bool
}

func (lp *linePrefixer) Write(p []byte) (int, error) {
	for _, b := range p {
		if lp.atLineStart {
			if _, err := lp.w.WriteString(lp.prefix); err != nil {
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
