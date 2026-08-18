package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/pmezard/go-difflib/difflib"
	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// dumpNodeIdentity is the stable display identity of one mounted dump file.
// Cluster is the dump's cluster macro (or the grouping fallback used by the
// dump index); Node is the node{} label or filename-derived node name.
type dumpNodeIdentity struct {
	Cluster        string
	RoutingCluster string
	Node           string
}

type dumpObjectSnapshot struct {
	Node         dumpNodeIdentity
	BasePath     string
	NodeHref     string
	Database     string
	DatabaseHref string
	Kind         string
	Object       string
	ObjectHref   string
	Signature    string
	Schema       *hclload.Schema
}

// dumpWebContext is a live, concurrency-safe index of every browsable object
// in every mounted node dump. Each webServer replaces its own snapshot after a
// reload, so object pages can show current cross-node presence without locking
// peers.
type dumpWebContext struct {
	mu       sync.RWMutex
	byServer map[string]map[string]dumpObjectSnapshot // base path -> db\x00kind\x00name -> snapshot
	nodes    map[string]dumpNodeIdentity              // base path -> mounted dump identity
	servers  []*webServer
	aliases  map[string]string // remote_servers alias -> physical dump cluster
}

type objectPresenceView struct {
	Cluster      string
	Node         string
	NodeHref     string
	Database     string
	DatabaseHref string
	Kind         string
	Object       string
	ObjectHref   string
	CompareHref  string
	Status       string
	MarkerClass  string
	Current      bool
	Different    bool
}

func newDumpWebContext(aliases map[string]string) *dumpWebContext {
	return &dumpWebContext{
		byServer: map[string]map[string]dumpObjectSnapshot{},
		nodes:    map[string]dumpNodeIdentity{},
		aliases:  aliases,
	}
}

func dumpClusterAliases(mappings map[string]validateDumpCluster) map[string]string {
	aliases := map[string]string{}
	for name, mapping := range mappings {
		if mapping.Kind == "alias" {
			aliases[name] = mapping.Base
		}
	}
	return aliases
}

func (s *webServer) attachDumpContext(ctx *dumpWebContext, node dumpNodeIdentity) error {
	s.dumpContext = ctx
	s.dumpNode = node
	if err := ctx.update(s.basePath, node, s.schema); err != nil {
		return err
	}
	ctx.mu.Lock()
	ctx.servers = append(ctx.servers, s)
	ctx.mu.Unlock()
	return nil
}

// maybeReloadAll refreshes every mounted dump before a cross-node comparison.
// Each server applies its own reload interval and TryLock throttle, so this is
// cheap between intervals and never makes concurrent requests wait on reload.
func (ctx *dumpWebContext) maybeReloadAll() {
	ctx.mu.RLock()
	servers := append([]*webServer(nil), ctx.servers...)
	ctx.mu.RUnlock()
	for _, server := range servers {
		server.maybeReload()
	}
}

// update atomically replaces one node's browsable-object snapshot. Tables mask
// ReplicatedMergeTree UUIDs embedded in zoo_path exactly as `drift` does; every
// other kind compares its resolved canonical HCL directly.
func (ctx *dumpWebContext) update(base string, node dumpNodeIdentity, schema *hclload.Schema) error {
	normalizedSchema := normalizedDumpSchema(schema)
	objects := map[string]dumpObjectSnapshot{}
	for di := range normalizedSchema.Databases {
		db := &normalizedSchema.Databases[di]
		add := func(kind, name string) error {
			signature, err := normalizedObjectSignature(db.Name, kind, name, db)
			if err != nil {
				return fmt.Errorf("render %s %s.%s: %w", kind, db.Name, name, err)
			}
			objects[dumpObjectKey(db.Name, kind, name)] = dumpObjectSnapshot{
				Node:         node,
				BasePath:     base,
				NodeHref:     base + "/",
				Database:     db.Name,
				DatabaseHref: base + "/#" + databaseAnchor(db.Name),
				Kind:         kind,
				Object:       name,
				ObjectHref:   base + objectHref(db.Name, kind, name),
				Signature:    signature,
				Schema:       normalizedSchema,
			}
			return nil
		}
		for _, table := range db.Tables {
			if err := add(hclload.KindTable, table.Name); err != nil {
				return err
			}
		}
		for _, mv := range db.MaterializedViews {
			if err := add(hclload.KindMaterializedView, mv.Name); err != nil {
				return err
			}
		}
		for _, view := range db.Views {
			if err := add(hclload.KindView, view.Name); err != nil {
				return err
			}
		}
		for _, dictionary := range db.Dictionaries {
			if err := add(hclload.KindDictionary, dictionary.Name); err != nil {
				return err
			}
		}
		for _, raw := range db.Raws {
			if err := add(hclload.KindRaw, raw.Name); err != nil {
				return err
			}
		}
	}

	ctx.mu.Lock()
	ctx.byServer[base] = objects
	ctx.nodes[base] = node
	ctx.mu.Unlock()
	return nil
}

// normalizedDumpSchema makes a comparison-only shallow clone whose database,
// table, and engine containers are independent from the live web schema. It can
// therefore apply drift's UUID masking without mutating request-visible state.
// Everything else is immutable after loading and can be shared safely.
func normalizedDumpSchema(schema *hclload.Schema) *hclload.Schema {
	normalized := *schema
	normalized.Databases = append([]hclload.DatabaseSpec(nil), schema.Databases...)
	for di := range normalized.Databases {
		sourceDB := &schema.Databases[di]
		db := &normalized.Databases[di]
		db.Tables = append([]hclload.TableSpec(nil), sourceDB.Tables...)
		for ti := range db.Tables {
			if sourceDB.Tables[ti].Engine == nil {
				continue
			}
			engine := *sourceDB.Tables[ti].Engine
			db.Tables[ti].Engine = &engine
		}
	}
	normalizeZKPaths(&normalized, "mask-uuid")
	return &normalized
}

func dumpObjectKey(database, kind, name string) string {
	return database + "\x00" + kind + "\x00" + name
}

type dumpObjectReview struct {
	TotalNodes       int
	TotalObjects     int
	UniformObjects   int
	DifferentObjects int
	PartialObjects   int
	Objects          []dumpObjectReviewView
}

type dumpObjectReviewView struct {
	Database     string
	DatabaseHref string
	Kind         string
	KindLabel    string
	Name         string
	ObjectHref   string
	PresentNodes int
	TotalNodes   int
	MissingNodes []dumpMissingNodeView
	Variants     []dumpSchemaVariantView
	Status       string
	MarkerClass  string
	Different    bool
	Partial      bool
	searchText   string
}

type dumpSchemaVariantView struct {
	Number      int
	Nodes       []objectCompareMatchView
	CompareHref string
}

type dumpMissingNodeView struct {
	Cluster  string
	Node     string
	NodeHref string
}

// objectReview returns the union of browsable objects in the dump, grouped by
// canonical schema signature. Absence is reported as deployment context but is
// not schema drift: different node roles legitimately contain different object
// sets, matching objectPresence's existing semantics.
func (ctx *dumpWebContext) objectReview() dumpObjectReview {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	review := dumpObjectReview{TotalNodes: len(ctx.nodes)}
	keys := map[string]bool{}
	for _, objects := range ctx.byServer {
		for key := range objects {
			keys[key] = true
		}
	}

	for key := range keys {
		var snapshots []dumpObjectSnapshot
		var missing []dumpMissingNodeView
		for base, node := range ctx.nodes {
			if snapshot, ok := ctx.byServer[base][key]; ok {
				snapshots = append(snapshots, snapshot)
				continue
			}
			missing = append(missing, dumpMissingNodeView{
				Cluster:  node.Cluster,
				Node:     node.Node,
				NodeHref: base + "/",
			})
		}
		if len(snapshots) == 0 {
			continue
		}
		sortObjectSnapshots(snapshots)
		sort.Slice(missing, func(i, j int) bool {
			if missing[i].Cluster != missing[j].Cluster {
				return missing[i].Cluster < missing[j].Cluster
			}
			return missing[i].Node < missing[j].Node
		})

		bySignature := map[string][]dumpObjectSnapshot{}
		for _, snapshot := range snapshots {
			bySignature[snapshot.Signature] = append(bySignature[snapshot.Signature], snapshot)
		}
		groups := make([][]dumpObjectSnapshot, 0, len(bySignature))
		for _, group := range bySignature {
			sortObjectSnapshots(group)
			groups = append(groups, group)
		}
		sort.Slice(groups, func(i, j int) bool {
			left, right := groups[i][0], groups[j][0]
			if left.Node.Cluster != right.Node.Cluster {
				return left.Node.Cluster < right.Node.Cluster
			}
			return left.Node.Node < right.Node.Node
		})

		first := snapshots[0]
		view := dumpObjectReviewView{
			Database:     first.Database,
			DatabaseHref: first.DatabaseHref,
			Kind:         first.Kind,
			KindLabel:    kindLabel(first.Kind),
			Name:         first.Object,
			ObjectHref:   first.ObjectHref,
			PresentNodes: len(snapshots),
			TotalNodes:   review.TotalNodes,
			MissingNodes: missing,
			Status:       "uniform",
			MarkerClass:  "same",
			Partial:      len(missing) > 0,
		}
		baseline := groups[0][0]
		for i, group := range groups {
			variant := dumpSchemaVariantView{Number: i + 1}
			if i > 0 {
				variant.CompareHref = objectCompareHref(
					baseline.BasePath, group[0].Node.Node, first.Database, first.Kind, first.Object,
				)
			}
			for _, snapshot := range group {
				variant.Nodes = append(variant.Nodes, objectCompareMatchView{
					Cluster:    snapshot.Node.Cluster,
					Node:       snapshot.Node.Node,
					NodeHref:   snapshot.NodeHref,
					ObjectHref: snapshot.ObjectHref,
				})
			}
			view.Variants = append(view.Variants, variant)
		}
		if len(view.Variants) > 1 {
			view.Status = "different"
			view.MarkerClass = "different"
			view.Different = true
			review.DifferentObjects++
		} else {
			review.UniformObjects++
		}
		if view.Partial {
			review.PartialObjects++
		}
		var searchParts = []string{view.Database, view.Kind, view.KindLabel, view.Name}
		for _, snapshot := range snapshots {
			searchParts = append(searchParts, snapshot.Node.Cluster, snapshot.Node.Node)
		}
		for _, node := range missing {
			searchParts = append(searchParts, node.Cluster, node.Node)
		}
		view.searchText = strings.ToLower(strings.Join(searchParts, " "))
		review.Objects = append(review.Objects, view)
	}

	review.TotalObjects = len(review.Objects)
	sort.Slice(review.Objects, func(i, j int) bool {
		left, right := review.Objects[i], review.Objects[j]
		if left.Database != right.Database {
			return left.Database < right.Database
		}
		if dumpObjectKindRank(left.Kind) != dumpObjectKindRank(right.Kind) {
			return dumpObjectKindRank(left.Kind) < dumpObjectKindRank(right.Kind)
		}
		return left.Name < right.Name
	})
	return review
}

func dumpObjectKindRank(kind string) int {
	switch kind {
	case hclload.KindTable:
		return 0
	case hclload.KindMaterializedView:
		return 1
	case hclload.KindView:
		return 2
	case hclload.KindDictionary:
		return 3
	case hclload.KindRaw:
		return 4
	default:
		return 5
	}
}

func normalizedObjectSignature(database, kind, name string, db *hclload.DatabaseSpec) (string, error) {
	if kind != hclload.KindTable {
		return hclload.RenderObjectHCL(database, kind, name, db)
	}
	table := findTable(db, name)
	if table == nil {
		return "", fmt.Errorf("table not found")
	}
	return normalizedTableSignature(database, *table)
}

func normalizedTableSignature(database string, table hclload.TableSpec) (string, error) {
	normalized := table
	if table.Engine != nil {
		engine := *table.Engine
		if engine.Decoded != nil {
			engine.Decoded = normalizeEngineZK(engine.Decoded, "mask-uuid")
		}
		normalized.Engine = &engine
	}
	db := &hclload.DatabaseSpec{Name: database, Tables: []hclload.TableSpec{normalized}}
	return hclload.RenderObjectHCL(database, hclload.KindTable, table.Name, db)
}

// objectPresence returns every dumped node containing the same object. The
// current node is the baseline; every other signature is marked same/different
// against it. Missing objects are intentionally omitted: this answers where the
// object exists without flooding role-specific pages with every unrelated node.
func (ctx *dumpWebContext) objectPresence(currentBase, database, kind, name string) []objectPresenceView {
	key := dumpObjectKey(database, kind, name)
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	current, ok := ctx.byServer[currentBase][key]
	if !ok {
		return nil
	}
	var out []objectPresenceView
	for base, objects := range ctx.byServer {
		peer, exists := objects[key]
		if !exists {
			continue
		}
		view := objectPresenceView{
			Cluster:      peer.Node.Cluster,
			Node:         peer.Node.Node,
			NodeHref:     peer.NodeHref,
			Database:     peer.Database,
			DatabaseHref: peer.DatabaseHref,
			Kind:         peer.Kind,
			Object:       peer.Object,
			ObjectHref:   peer.ObjectHref,
		}
		switch {
		case base == currentBase:
			view.Status = "current"
			view.MarkerClass = "current"
			view.Current = true
		case peer.Signature == current.Signature:
			view.Status = "same"
			view.MarkerClass = "same"
		default:
			view.Status = "different"
			view.MarkerClass = "different"
			view.Different = true
			view.CompareHref = objectCompareHref(currentBase, peer.Node.Node, database, kind, name)
		}
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Cluster != out[j].Cluster {
			return out[i].Cluster < out[j].Cluster
		}
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].ObjectHref < out[j].ObjectHref
	})
	return out
}

func objectCompareHref(currentBase, peer, database, kind, name string) string {
	query := url.Values{
		"peer":     []string{peer},
		"database": []string{database},
		"kind":     []string{kind},
		"name":     []string{name},
	}
	return currentBase + "/compare?" + query.Encode()
}

type objectCompareSideView struct {
	Cluster string
	Node    string
	Href    string
	Matches []objectCompareMatchView
}

type objectCompareMatchView struct {
	Cluster    string
	Node       string
	NodeHref   string
	ObjectHref string
}

type schemaDiffLineView struct {
	Class string
	Text  string
}

type objectCompareData struct {
	Title        string
	Base         string
	Label        string
	Database     string
	DatabaseHref string
	KindLabel    string
	Name         string
	SwapHref     string
	PatchHref    string
	Current      objectCompareSideView
	Peer         objectCompareSideView
	Same         bool
	Lines        []schemaDiffLineView
	ShowPatch    bool
	PatchSQL     string
	PatchUnsafe  []string
}

type dumpObjectComparison struct {
	Current        dumpObjectSnapshot
	Peer           dumpObjectSnapshot
	CurrentMatches []dumpObjectSnapshot
	PeerMatches    []dumpObjectSnapshot
}

// objectComparison returns the current node's canonical HCL and the requested
// peer's canonical HCL. Dump node names are globally unique, as enforced while
// building the dump server, so the peer query parameter identifies one mount.
func (ctx *dumpWebContext) objectComparison(currentBase, peerNode, database, kind, name string) (dumpObjectComparison, bool) {
	key := dumpObjectKey(database, kind, name)
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	current, ok := ctx.byServer[currentBase][key]
	if !ok {
		return dumpObjectComparison{}, false
	}
	var peer dumpObjectSnapshot
	var snapshots []dumpObjectSnapshot
	for _, objects := range ctx.byServer {
		if snapshot, exists := objects[key]; exists {
			snapshots = append(snapshots, snapshot)
			if snapshot.Node.Node == peerNode {
				peer = snapshot
			}
		}
	}
	if peer.Node.Node == "" {
		return dumpObjectComparison{}, false
	}

	comparison := dumpObjectComparison{Current: current, Peer: peer}
	for _, snapshot := range snapshots {
		if snapshot.Signature == current.Signature {
			comparison.CurrentMatches = append(comparison.CurrentMatches, snapshot)
		}
		if snapshot.Signature == peer.Signature {
			comparison.PeerMatches = append(comparison.PeerMatches, snapshot)
		}
	}
	sortObjectSnapshots(comparison.CurrentMatches)
	sortObjectSnapshots(comparison.PeerMatches)
	return comparison, true
}

func sortObjectSnapshots(snapshots []dumpObjectSnapshot) {
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].Node.Cluster != snapshots[j].Node.Cluster {
			return snapshots[i].Node.Cluster < snapshots[j].Node.Cluster
		}
		if snapshots[i].Node.Node != snapshots[j].Node.Node {
			return snapshots[i].Node.Node < snapshots[j].Node.Node
		}
		return snapshots[i].ObjectHref < snapshots[j].ObjectHref
	})
}

func objectCompareSide(snapshot dumpObjectSnapshot, matches []dumpObjectSnapshot) objectCompareSideView {
	view := objectCompareSideView{
		Cluster: snapshot.Node.Cluster,
		Node:    snapshot.Node.Node,
		Href:    snapshot.ObjectHref,
	}
	for _, match := range matches {
		view.Matches = append(view.Matches, objectCompareMatchView{
			Cluster:    match.Node.Cluster,
			Node:       match.Node.Node,
			NodeHref:   match.NodeHref,
			ObjectHref: match.ObjectHref,
		})
	}
	return view
}

func objectPatchHref(currentBase, peer, database, kind, name string) string {
	return objectCompareHref(currentBase, peer, database, kind, name) + "&patch=1#patch-to-uniform"
}

// objectPatchSQL uses the same Diff -> GenerateSQL -> object comparison path as
// the CLI, in right-to-left order. Only the selected object's operations are
// rendered and each statement is parser-beautified for the preview. Manual
// operations stay commented, and unsafe/unexpressible changes are returned
// separately so the UI cannot imply that a partial patch is enough.
func objectPatchSQL(from, to dumpObjectSnapshot, database, kind, name string) (string, []string) {
	cs := hclload.Diff(from.Schema, to.Schema)
	gen := hclload.GenerateSQL(cs)
	comparisons := hclload.BuildObjectComparisons(cs, gen, from.Schema, to.Schema)
	for _, comparison := range comparisons {
		if comparison.Database != database || comparison.ObjectType != kind || comparison.Object != name {
			continue
		}

		var statements []string
		for _, operation := range comparison.Operations {
			statement := operation.SQL
			if pretty, ok := hclload.BeautifySQL(statement); ok {
				statement = pretty
			}
			statement = strings.TrimSuffix(strings.TrimSpace(statement), ";") + ";"
			if operation.Manual {
				statement = "-- MANUAL: " + strings.ReplaceAll(statement, "\n", "\n-- ")
			}
			statements = append(statements, statement)
		}
		var unsafe []string
		if comparison.UnsafeReason != "" {
			unsafe = append(unsafe, comparison.UnsafeReason)
		}
		if comparison.Error != "" {
			unsafe = append(unsafe, comparison.Error)
		}
		if len(statements) == 0 {
			statements = append(statements, "-- no automatic SQL was generated for this change")
			if len(unsafe) == 0 {
				unsafe = append(unsafe, "the migration planner could not express this object change")
			}
		}
		return strings.Join(statements, "\n\n"), unsafe
	}
	return "-- no automatic SQL was generated for this change", []string{
		"the migration planner could not isolate this object change",
	}
}

func schemaDiffLines(current, peer dumpObjectSnapshot) ([]schemaDiffLineView, error) {
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(current.Signature),
		B:        difflib.SplitLines(peer.Signature),
		FromFile: current.Node.Cluster + " / " + current.Node.Node + " (baseline)",
		ToFile:   peer.Node.Cluster + " / " + peer.Node.Node,
		Context:  3,
	})
	if err != nil {
		return nil, err
	}
	if diff == "" {
		return nil, nil
	}

	lines := strings.Split(strings.TrimSuffix(diff, "\n"), "\n")
	out := make([]schemaDiffLineView, 0, len(lines))
	for _, line := range lines {
		class := "diff-context"
		switch {
		case strings.HasPrefix(line, "--- "), strings.HasPrefix(line, "+++ "):
			class = "diff-header"
		case strings.HasPrefix(line, "@@"):
			class = "diff-hunk"
		case strings.HasPrefix(line, "-"):
			class = "diff-delete"
		case strings.HasPrefix(line, "+"):
			class = "diff-add"
		}
		out = append(out, schemaDiffLineView{Class: class, Text: line})
	}
	return out, nil
}

func (s *webServer) handleCompare(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/compare" || s.dumpContext == nil {
		s.notFound(w)
		return
	}
	s.dumpContext.maybeReloadAll()

	peerNode := r.URL.Query().Get("peer")
	database := r.URL.Query().Get("database")
	kind := r.URL.Query().Get("kind")
	name := r.URL.Query().Get("name")
	if peerNode == "" || database == "" || kind == "" || name == "" {
		s.notFound(w)
		return
	}
	comparison, ok := s.dumpContext.objectComparison(s.basePath, peerNode, database, kind, name)
	if !ok {
		s.notFound(w)
		return
	}
	current := comparison.Current
	peer := comparison.Peer
	lines, err := schemaDiffLines(current, peer)
	if err != nil {
		http.Error(w, "render schema diff: "+err.Error(), http.StatusInternalServerError)
		return
	}

	data := objectCompareData{
		Title:        "Schema comparison: " + name,
		Base:         s.basePath,
		Label:        s.label,
		Database:     database,
		DatabaseHref: current.DatabaseHref,
		KindLabel:    kindLabel(kind),
		Name:         name,
		SwapHref:     objectCompareHref(peer.BasePath, current.Node.Node, database, kind, name),
		PatchHref:    objectPatchHref(s.basePath, peer.Node.Node, database, kind, name),
		Current:      objectCompareSide(current, comparison.CurrentMatches),
		Peer:         objectCompareSide(peer, comparison.PeerMatches),
		Same:         current.Signature == peer.Signature,
		Lines:        lines,
	}
	if r.URL.Query().Get("patch") == "1" {
		data.ShowPatch = true
		data.PatchSQL, data.PatchUnsafe = objectPatchSQL(peer, current, database, kind, name)
	}
	s.render(w, s.tmplCompare, data)
}

// referenceHref resolves a dependency using the same topology order as schema
// validation. Local declarations win. Distributed remotes then resolve through
// their named cluster (including inferred aliases); MV read sources may resolve
// from any mapped sibling cluster. Write targets remain local.
func (ctx *dumpWebContext) referenceHref(currentBase string, dep hclload.Dependency) string {
	key := dumpObjectKey(dep.To.Database, hclload.KindTable, dep.To.Name)
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	if local, ok := ctx.byServer[currentBase][key]; ok {
		return local.ObjectHref
	}
	switch dep.Kind {
	case hclload.DepDistributedRemote:
		cluster := ctx.resolveCluster(dep.Cluster)
		return firstSnapshotHref(ctx.byServer, key, func(snapshot dumpObjectSnapshot) bool {
			return snapshot.Node.RoutingCluster == cluster
		})
	case hclload.DepMVSource, hclload.DepViewSource:
		return firstSnapshotHref(ctx.byServer, key, func(snapshot dumpObjectSnapshot) bool {
			return snapshot.Node.RoutingCluster != ""
		})
	default:
		return ""
	}
}

func (ctx *dumpWebContext) resolveCluster(name string) string {
	seen := map[string]bool{}
	for !seen[name] {
		seen[name] = true
		base, ok := ctx.aliases[name]
		if !ok {
			return name
		}
		name = base
	}
	return name
}

func firstSnapshotHref(byServer map[string]map[string]dumpObjectSnapshot, key string, match func(dumpObjectSnapshot) bool) string {
	var matches []dumpObjectSnapshot
	for _, objects := range byServer {
		if snapshot, ok := objects[key]; ok && match(snapshot) {
			matches = append(matches, snapshot)
		}
	}
	if len(matches) == 0 {
		return ""
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Node.Cluster != matches[j].Node.Cluster {
			return matches[i].Node.Cluster < matches[j].Node.Cluster
		}
		return matches[i].Node.Node < matches[j].Node.Node
	})
	return matches[0].ObjectHref
}

// resolveFlowReferences copies the precomputed flows and fills undeclared
// stages from loaded dump clusters. The flow builder remains schema-local;
// this presentation pass applies the same cross-cluster rules as validation
// without merging node schemas or changing dependency construction.
func (ctx *dumpWebContext) resolveFlowReferences(currentBase string, flows []flow, deps []hclload.Dependency) []flow {
	out := make([]flow, len(flows))
	for i := range flows {
		out[i] = flows[i]
		out[i].Stages = append([]flowStage(nil), flows[i].Stages...)
		for j := range out[i].Stages {
			stage := &out[i].Stages[j]
			if stage.Declared {
				continue
			}
			ref := hclload.ObjectRef{Database: stage.Database, Name: stage.Name}
			for _, dep := range deps {
				if dep.To != ref {
					continue
				}
				if href := ctx.referenceHref(currentBase, dep); href != "" {
					stage.Href = href
					stage.HrefFull = true
					stage.Declared = true
					break
				}
			}
		}
	}
	return out
}
