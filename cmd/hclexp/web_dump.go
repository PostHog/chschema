package main

import (
	"fmt"
	"sort"
	"sync"

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

type dumpTableSnapshot struct {
	Node      dumpNodeIdentity
	Href      string
	Signature string
}

// dumpWebContext is a live, concurrency-safe index of every table in every
// mounted node dump. Each webServer replaces its own snapshot after a reload,
// so table pages can show current cross-node presence without locking peers.
type dumpWebContext struct {
	mu       sync.RWMutex
	byServer map[string]map[string]dumpTableSnapshot // base path -> db\x00table -> snapshot
	servers  []*webServer
	aliases  map[string]string // remote_servers alias -> physical dump cluster
}

type tablePresenceView struct {
	Cluster     string
	Node        string
	Href        string
	Status      string
	MarkerClass string
	Current     bool
	Different   bool
}

func newDumpWebContext(aliases map[string]string) *dumpWebContext {
	return &dumpWebContext{
		byServer: map[string]map[string]dumpTableSnapshot{},
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

// update atomically replaces one node's table snapshot. ReplicatedMergeTree
// UUIDs embedded in zoo_path are masked exactly as they are for `drift`, so
// per-node UUID expansion does not produce a false schema difference.
func (ctx *dumpWebContext) update(base string, node dumpNodeIdentity, schema *hclload.Schema) error {
	tables := map[string]dumpTableSnapshot{}
	for di := range schema.Databases {
		db := &schema.Databases[di]
		for _, table := range db.Tables {
			signature, err := normalizedTableSignature(db.Name, table)
			if err != nil {
				return fmt.Errorf("render %s.%s: %w", db.Name, table.Name, err)
			}
			tables[indexKey(db.Name, table.Name)] = dumpTableSnapshot{
				Node:      node,
				Href:      base + objectHref(db.Name, hclload.KindTable, table.Name),
				Signature: signature,
			}
		}
	}

	ctx.mu.Lock()
	ctx.byServer[base] = tables
	ctx.mu.Unlock()
	return nil
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

// tablePresence returns every dumped node containing database.table. The
// current node is the baseline; every other signature is marked same/different
// against it. Missing tables are intentionally omitted: this answers where the
// object exists without flooding role-specific tables with every unrelated node.
func (ctx *dumpWebContext) tablePresence(currentBase, database, name string) []tablePresenceView {
	key := indexKey(database, name)
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	current, ok := ctx.byServer[currentBase][key]
	if !ok {
		return nil
	}
	var out []tablePresenceView
	for base, tables := range ctx.byServer {
		peer, exists := tables[key]
		if !exists {
			continue
		}
		view := tablePresenceView{
			Cluster: peer.Node.Cluster,
			Node:    peer.Node.Node,
			Href:    peer.Href,
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
		}
		out = append(out, view)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Current != out[j].Current {
			return out[i].Current
		}
		if out[i].Cluster != out[j].Cluster {
			return out[i].Cluster < out[j].Cluster
		}
		return out[i].Node < out[j].Node
	})
	return out
}

// referenceHref resolves a dependency using the same topology order as schema
// validation. Local declarations win. Distributed remotes then resolve through
// their named cluster (including inferred aliases); MV read sources may resolve
// from any mapped sibling cluster. Write targets remain local.
func (ctx *dumpWebContext) referenceHref(currentBase string, dep hclload.Dependency) string {
	key := indexKey(dep.To.Database, dep.To.Name)
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()

	if local, ok := ctx.byServer[currentBase][key]; ok {
		return local.Href
	}
	switch dep.Kind {
	case hclload.DepDistributedRemote:
		cluster := ctx.resolveCluster(dep.Cluster)
		return firstSnapshotHref(ctx.byServer, key, func(snapshot dumpTableSnapshot) bool {
			return snapshot.Node.RoutingCluster == cluster
		})
	case hclload.DepMVSource, hclload.DepViewSource:
		return firstSnapshotHref(ctx.byServer, key, func(snapshot dumpTableSnapshot) bool {
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

func firstSnapshotHref(byServer map[string]map[string]dumpTableSnapshot, key string, match func(dumpTableSnapshot) bool) string {
	var matches []dumpTableSnapshot
	for _, tables := range byServer {
		if snapshot, ok := tables[key]; ok && match(snapshot) {
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
	return matches[0].Href
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
