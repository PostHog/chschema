package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// validateDumpOptions controls validation of a directory of independent
// per-node dumps. The schemas are never composed as layers: peer declarations
// are unioned only inside the derived ClusterSet.
type validateDumpOptions struct {
	Glob           string
	Skip           hclload.SkipSet
	Validate       hclload.ValidateOptions
	ClusterEntries []clusterEntry
	Exclude        *hclload.ExcludeMatcher
}

// validateDumpCluster describes one mapping used by topology validation. Kind
// is schema, alias, or absent; Source is dump, inferred, or explicit.
type validateDumpCluster struct {
	Name    string   `json:"name"`
	Kind    string   `json:"kind"`
	Source  string   `json:"source"`
	Base    string   `json:"base,omitempty"`
	Stack   string   `json:"stack,omitempty"`
	Members []string `json:"members,omitempty"`
	Objects int      `json:"objects,omitempty"`
}

type validateDumpUnmappedCluster struct {
	Name         string   `json:"name"`
	ReferencedBy []string `json:"referenced_by"`
}

type validateDumpFinding struct {
	Object  string `json:"object,omitempty"`
	Missing string `json:"missing,omitempty"`
	Kind    string `json:"kind,omitempty"`
	Cluster string `json:"cluster,omitempty"`
	Reason  string `json:"reason"`
}

type validateDumpNode struct {
	Node             string                `json:"node"`
	File             string                `json:"file"`
	Cluster          string                `json:"cluster,omitempty"`
	UnmappedClusters []string              `json:"unmapped_clusters,omitempty"`
	Errors           []validateDumpFinding `json:"errors"`
}

type validateDumpSummary struct {
	Nodes            int `json:"nodes"`
	Clusters         int `json:"clusters"`
	NodesFailed      int `json:"nodes_failed"`
	UnmappedClusters int `json:"unmapped_clusters"`
	Errors           int `json:"errors"`
}

type validateDumpDoc struct {
	Dump             string                        `json:"dump"`
	Glob             string                        `json:"glob"`
	Clusters         []validateDumpCluster         `json:"clusters"`
	UnmappedClusters []validateDumpUnmappedCluster `json:"unmapped_clusters"`
	UnclusteredNodes []string                      `json:"unclustered_nodes,omitempty"`
	Nodes            []validateDumpNode            `json:"nodes"`
	Summary          validateDumpSummary           `json:"summary"`
}

// Known remote_servers suffixes used for aliases of a physical cluster. The
// longest names go first so future overlapping suffixes remain deterministic.
var dumpClusterAliasSuffixes = []string{
	"_primary_replica",
	"_single_shard",
	"_writable",
}

// validateTopologyDump loads every selected dump independently, derives a
// cluster union from macros.cluster, infers well-known aliases, applies
// explicit -cluster entries last, and validates every node against that set.
func validateTopologyDump(dir string, opts validateDumpOptions) (validateDumpDoc, error) {
	glob := opts.Glob
	if glob == "" {
		glob = "*"
	}
	nodes, err := loadDriftNodes(dir, glob)
	if err != nil {
		return validateDumpDoc{}, err
	}
	if len(nodes) == 0 {
		return validateDumpDoc{}, fmt.Errorf("no .hcl files in %s match %q", dir, glob)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	if opts.Exclude != nil {
		for i := range nodes {
			hclload.FilterSchema(nodes[i].Schema, opts.Exclude)
		}
	}

	cs, mappings, unclustered, err := deriveDumpClusterSet(nodes, opts.ClusterEntries)
	if err != nil {
		return validateDumpDoc{}, err
	}
	references := dumpClusterReferences(nodes, opts.Skip)
	addInferredDumpClusterAliases(&cs, mappings, references)

	unknown := map[string]bool{}
	unmapped := []validateDumpUnmappedCluster{}
	for _, name := range sortedMapKeys(references) {
		if dumpMappingResolves(name, mappings) {
			continue
		}
		unknown[name] = true
		members := sortedMapKeys(references[name])
		unmapped = append(unmapped, validateDumpUnmappedCluster{Name: name, ReferencedBy: members})
	}

	doc := validateDumpDoc{
		Dump:             dir,
		Glob:             glob,
		Clusters:         sortedDumpMappings(mappings),
		UnmappedClusters: unmapped,
		UnclusteredNodes: unclustered,
		Nodes:            make([]validateDumpNode, 0, len(nodes)),
	}
	failedNodes := map[string]bool{}
	for _, u := range unmapped {
		for _, node := range u.ReferencedBy {
			failedNodes[node] = true
		}
	}

	for _, node := range nodes {
		result := validateDumpNode{
			Node:    node.Name,
			File:    node.File,
			Cluster: node.Macros["cluster"],
			Errors:  []validateDumpFinding{},
		}
		for cluster, refs := range references {
			if unknown[cluster] && refs[node.Name] {
				result.UnmappedClusters = append(result.UnmappedClusters, cluster)
			}
		}
		sort.Strings(result.UnmappedClusters)

		errs := hclload.ValidateOpts(node.Schema.Databases, opts.Skip, cs, opts.Validate)
		for _, validationErr := range errs {
			// Unknown cluster names are emitted once at topology level. Keep the
			// affected cluster on each node, but suppress the repeated per-proxy
			// "no mapping" finding.
			if validationErr.Kind == hclload.DepDistributedRemote && unknown[validationErr.Cluster] {
				continue
			}
			result.Errors = append(result.Errors, dumpFinding(validationErr))
		}
		if len(result.Errors) > 0 {
			failedNodes[node.Name] = true
		}
		doc.Nodes = append(doc.Nodes, result)
	}

	doc.Summary = validateDumpSummary{
		Nodes:            len(nodes),
		Clusters:         len(mappings),
		NodesFailed:      len(failedNodes),
		UnmappedClusters: len(unmapped),
		Errors:           len(unmapped),
	}
	for _, node := range doc.Nodes {
		doc.Summary.Errors += len(node.Errors)
	}
	return doc, nil
}

// addInferredDumpClusterAliases adds the well-known remote_servers aliases
// referenced by loaded nodes to both the validation ClusterSet and its
// descriptive mapping. Web dump browsing reuses this so its links and
// validation resolve the same topology as `validate -dump`.
func addInferredDumpClusterAliases(cs *hclload.ClusterSet, mappings map[string]validateDumpCluster, references map[string]map[string]bool) {
	for _, name := range sortedMapKeys(references) {
		if _, exists := mappings[name]; exists {
			continue
		}
		if base := inferredDumpClusterBase(name, mappings); base != "" {
			cs.AddAlias(name, base)
			mappings[name] = validateDumpCluster{
				Name: name, Kind: "alias", Source: "inferred", Base: base,
			}
		}
	}
}

// deriveDumpClusterSet groups nodes by macros.cluster and registers each
// cluster against the union of its members' database objects. Explicit
// mappings are applied last and replace the derived mapping with the same name.
func deriveDumpClusterSet(nodes []driftNode, entries []clusterEntry) (hclload.ClusterSet, map[string]validateDumpCluster, []string, error) {
	cs := hclload.NewClusterSet()
	groups := map[string][]driftNode{}
	var unclustered []string
	for _, node := range nodes {
		cluster := node.Macros["cluster"]
		if cluster == "" {
			unclustered = append(unclustered, node.Name)
			continue
		}
		groups[cluster] = append(groups[cluster], node)
	}
	sort.Strings(unclustered)

	mappings := map[string]validateDumpCluster{}
	for _, name := range sortedMapKeys(groups) {
		members := groups[name]
		sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
		var dbs []hclload.DatabaseSpec
		memberNames := make([]string, 0, len(members))
		for _, member := range members {
			dbs = append(dbs, member.Schema.Databases...)
			memberNames = append(memberNames, member.Name)
		}
		cs.Add(name, dbs)
		mappings[name] = validateDumpCluster{
			Name: name, Kind: "schema", Source: "dump", Members: memberNames,
			Objects: countDeclaredDumpObjects(dbs),
		}
	}

	if err := applyClusterEntries(&cs, entries); err != nil {
		return hclload.ClusterSet{}, nil, nil, err
	}
	for _, entry := range entries {
		mapping := validateDumpCluster{Name: entry.name, Source: "explicit", Stack: entry.stack}
		switch {
		case entry.stack == absentStack:
			mapping.Kind = "absent"
		case strings.HasPrefix(entry.stack, aliasPrefix):
			mapping.Kind = "alias"
			mapping.Base = strings.TrimPrefix(entry.stack, aliasPrefix)
		default:
			mapping.Kind = "schema"
		}
		mappings[entry.name] = mapping
	}
	return cs, mappings, unclustered, nil
}

func dumpClusterReferences(nodes []driftNode, skip hclload.SkipSet) map[string]map[string]bool {
	refs := map[string]map[string]bool{}
	for _, node := range nodes {
		for _, db := range node.Schema.Databases {
			for _, table := range db.Tables {
				if skip.Skips(hclload.ObjectRef{Database: db.Name, Name: table.Name}) {
					continue
				}
				if table.Engine == nil {
					continue
				}
				engine, ok := table.Engine.Decoded.(hclload.EngineDistributed)
				if !ok || engine.ClusterName == "" {
					continue
				}
				if refs[engine.ClusterName] == nil {
					refs[engine.ClusterName] = map[string]bool{}
				}
				refs[engine.ClusterName][node.Name] = true
			}
		}
	}
	return refs
}

func inferredDumpClusterBase(name string, mappings map[string]validateDumpCluster) string {
	for _, suffix := range dumpClusterAliasSuffixes {
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		base := strings.TrimSuffix(name, suffix)
		if base != "" && dumpMappingResolves(base, mappings) {
			return base
		}
	}
	return ""
}

func dumpMappingResolves(name string, mappings map[string]validateDumpCluster) bool {
	seen := map[string]bool{}
	for {
		if seen[name] {
			return false
		}
		seen[name] = true
		mapping, ok := mappings[name]
		if !ok {
			return false
		}
		switch mapping.Kind {
		case "schema", "absent":
			return true
		case "alias":
			name = mapping.Base
		default:
			return false
		}
	}
}

func sortedDumpMappings(mappings map[string]validateDumpCluster) []validateDumpCluster {
	out := make([]validateDumpCluster, 0, len(mappings))
	for _, name := range sortedMapKeys(mappings) {
		out = append(out, mappings[name])
	}
	return out
}

func countDeclaredDumpObjects(dbs []hclload.DatabaseSpec) int {
	seen := map[string]bool{}
	for _, db := range dbs {
		for _, table := range db.Tables {
			seen[db.Name+"\x00"+table.Name] = true
		}
		for _, mv := range db.MaterializedViews {
			seen[db.Name+"\x00"+mv.Name] = true
		}
		for _, view := range db.Views {
			seen[db.Name+"\x00"+view.Name] = true
		}
		for _, raw := range db.Raws {
			seen[db.Name+"\x00"+raw.Name] = true
		}
	}
	return len(seen)
}

func dumpFinding(err hclload.ValidationError) validateDumpFinding {
	return validateDumpFinding{
		Object: err.Object.String(), Missing: err.Missing.String(), Kind: err.Kind,
		Cluster: err.Cluster, Reason: err.Reason,
	}
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func renderValidateDumpText(w io.Writer, doc validateDumpDoc) {
	fmt.Fprintln(w, "derived cluster map:")
	for _, cluster := range doc.Clusters {
		switch cluster.Kind {
		case "schema":
			if cluster.Source == "dump" {
				fmt.Fprintf(w, "  %s: %d nodes, %d objects (%s)\n",
					cluster.Name, len(cluster.Members), cluster.Objects, strings.Join(cluster.Members, ", "))
			} else {
				fmt.Fprintf(w, "  %s: explicit schema %s\n", cluster.Name, cluster.Stack)
			}
		case "alias":
			fmt.Fprintf(w, "  %s: alias of %s (%s)\n", cluster.Name, cluster.Base, cluster.Source)
		case "absent":
			fmt.Fprintf(w, "  %s: @absent (%s)\n", cluster.Name, cluster.Source)
		}
	}
	for _, node := range doc.UnclusteredNodes {
		fmt.Fprintf(w, "warning: node %s has no macros.cluster and contributes to no derived cluster\n", node)
	}
	for _, cluster := range doc.UnmappedClusters {
		fmt.Fprintf(w, "validation error: cluster %q is referenced by %s but has no node in the dump and no explicit mapping\n",
			cluster.Name, strings.Join(cluster.ReferencedBy, ", "))
	}
	for _, node := range doc.Nodes {
		problems := len(node.Errors) + len(node.UnmappedClusters)
		if problems == 0 {
			fmt.Fprintf(w, "node %s [%s] — OK\n", node.Node, node.Cluster)
			continue
		}
		fmt.Fprintf(w, "node %s [%s] — %d problems\n", node.Node, node.Cluster, problems)
		for _, cluster := range node.UnmappedClusters {
			fmt.Fprintf(w, "  unmapped cluster: %s\n", cluster)
		}
		for _, finding := range node.Errors {
			fmt.Fprintf(w, "  validation error: %s\n", finding.Reason)
		}
	}
	fmt.Fprintf(w, "\nsummary: %d nodes, %d clusters, %d failed nodes, %d unmapped clusters, %d errors\n",
		doc.Summary.Nodes, doc.Summary.Clusters, doc.Summary.NodesFailed,
		doc.Summary.UnmappedClusters, doc.Summary.Errors)
}

func runValidateDump(dir, format string, opts validateDumpOptions) {
	doc, err := validateTopologyDump(dir, opts)
	if err != nil {
		slog.Error("failed to validate dump", "dir", dir, "err", err)
		os.Exit(1)
	}
	if format == "json" {
		out, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			slog.Error("failed to render dump validation JSON", "dir", dir, "err", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	} else {
		renderValidateDumpText(os.Stdout, doc)
	}
	if doc.Summary.Errors > 0 {
		os.Exit(1)
	}
}
