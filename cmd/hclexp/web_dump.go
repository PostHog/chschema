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
	Cluster string
	Node    string
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

func newDumpWebContext() *dumpWebContext {
	return &dumpWebContext{byServer: map[string]map[string]dumpTableSnapshot{}}
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
