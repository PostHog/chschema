package main

import (
	"fmt"
	"net/http"
	"sort"

	hclload "github.com/posthog/chschema/internal/loader/hcl"
)

// A flowStage is one node in a data-flow chain (a table or materialized view).
type flowStage struct {
	Kind      string // object kind: table | materialized_view | raw
	KindLabel string
	Engine    string // engine kind for tables (kafka, distributed, ...); "" for MVs
	Database  string
	Name      string
	Detail    string // optional subtitle (topic=…, cluster=…)
	Href      string // detail-page link; "" when the object isn't declared
	Declared  bool
	EdgeLabel string // label on the arrow leading into this stage (empty for the first)
	Problems  []problemView
}

// A flow is a single linear ingestion chain from a source to a terminal table.
type flow struct {
	Anchor      string
	Stages      []flowStage
	HasProblems bool
}

type flowsData struct {
	Title          string
	Flows          []flow
	GlobalProblems []problemView
}

// flowBuilder holds the indexes used to reconstruct flows from the dependency
// graph and the resolved schema.
type flowBuilder struct {
	kindIndex  map[string]string              // ref -> object kind
	engineKind map[string]string              // ref -> engine kind (tables only)
	detail     map[string]string              // ref -> stage subtitle
	readBy     map[string][]hclload.ObjectRef // source table ref -> MVs reading it
	mvDest     map[string]hclload.ObjectRef   // MV ref -> its target
	distRemote map[string]hclload.ObjectRef   // Distributed ref -> its storage table
	produced   map[string]bool                // refs that are an MV/Distributed target
}

// buildFlows reconstructs every MV ingestion chain in the schema and returns the
// flows plus a map from each participating object ref to its (first) flow anchor.
func buildFlows(schema *hclload.Schema, deps []hclload.Dependency, kindIndex map[string]string) ([]flow, map[string]string) {
	b := &flowBuilder{
		kindIndex:  kindIndex,
		engineKind: map[string]string{},
		detail:     map[string]string{},
		readBy:     map[string][]hclload.ObjectRef{},
		mvDest:     map[string]hclload.ObjectRef{},
		distRemote: map[string]hclload.ObjectRef{},
		produced:   map[string]bool{},
	}

	for i := range schema.Databases {
		db := &schema.Databases[i]
		for j := range db.Tables {
			t := &db.Tables[j]
			ref := indexKey(db.Name, t.Name)
			if t.Engine != nil {
				b.engineKind[ref] = t.Engine.Kind
				b.detail[ref] = engineDetail(t.Engine)
			}
		}
	}

	for _, d := range deps {
		switch d.Kind {
		case hclload.DepMVSource:
			to := indexKey(d.To.Database, d.To.Name)
			b.readBy[to] = append(b.readBy[to], d.From)
		case hclload.DepMVDest:
			b.mvDest[indexKey(d.From.Database, d.From.Name)] = d.To
			b.produced[indexKey(d.To.Database, d.To.Name)] = true
		case hclload.DepDistributedRemote:
			b.distRemote[indexKey(d.From.Database, d.From.Name)] = d.To
			b.produced[indexKey(d.To.Database, d.To.Name)] = true
		}
	}

	// Roots: source tables read by an MV that are not themselves produced by one.
	var roots []hclload.ObjectRef
	for ref, mvs := range b.readBy {
		if len(mvs) == 0 || b.produced[ref] {
			continue
		}
		// Recover an ObjectRef from any reading MV's edge target.
		roots = append(roots, refOfKey(ref))
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].String() < roots[j].String() })

	var flows []flow
	for _, root := range roots {
		b.walk(root, []flowStage{b.tableStage(root, "")}, map[string]bool{indexKey(root.Database, root.Name): true}, &flows)
	}

	anchorByRef := map[string]string{}
	for i := range flows {
		flows[i].Anchor = fmt.Sprintf("flow-%d", i+1)
		for _, st := range flows[i].Stages {
			ref := indexKey(st.Database, st.Name)
			if _, seen := anchorByRef[ref]; !seen {
				anchorByRef[ref] = flows[i].Anchor
			}
		}
	}
	return flows, anchorByRef
}

// walk extends the current chain (whose last stage is the table tableRef) through
// every MV that reads it, appending completed root→leaf paths to out.
func (b *flowBuilder) walk(tableRef hclload.ObjectRef, stages []flowStage, visited map[string]bool, out *[]flow) {
	consumers := b.readBy[indexKey(tableRef.Database, tableRef.Name)]
	if len(consumers) == 0 {
		*out = append(*out, flow{Stages: stages})
		return
	}
	sort.Slice(consumers, func(i, j int) bool { return consumers[i].String() < consumers[j].String() })

	for _, mv := range consumers {
		mvKey := indexKey(mv.Database, mv.Name)
		if visited[mvKey] {
			continue // cycle guard
		}
		dest, ok := b.mvDest[mvKey]
		if !ok {
			// MV with no recorded destination; end the chain at the MV.
			*out = append(*out, flow{Stages: append(clone(stages), b.mvStage(mv, "reads"))})
			continue
		}
		stagesA := append(clone(stages), b.mvStage(mv, "reads"))
		destKey := indexKey(dest.Database, dest.Name)

		if b.engineKind[destKey] == "distributed" {
			stagesB := append(stagesA, b.tableStage(dest, "writes to"))
			remote, hasRemote := b.distRemote[destKey]
			if !hasRemote {
				*out = append(*out, flow{Stages: stagesB})
				continue
			}
			stagesC := append(stagesB, b.tableStage(remote, "forwards to"))
			b.descend(remote, stagesC, visited, mvKey, out)
			continue
		}

		stagesB := append(stagesA, b.tableStage(dest, "writes to"))
		b.descend(dest, stagesB, visited, mvKey, out)
	}
}

// descend continues a chain from the next storage table, carrying a fresh visited
// set (with the just-used MV added) so sibling branches stay independent.
func (b *flowBuilder) descend(next hclload.ObjectRef, stages []flowStage, visited map[string]bool, usedMV string, out *[]flow) {
	nextKey := indexKey(next.Database, next.Name)
	if visited[nextKey] {
		*out = append(*out, flow{Stages: stages})
		return
	}
	v := cloneSet(visited)
	v[usedMV] = true
	v[nextKey] = true
	b.walk(next, stages, v, out)
}

func (b *flowBuilder) tableStage(ref hclload.ObjectRef, edge string) flowStage {
	key := indexKey(ref.Database, ref.Name)
	kind := b.kindIndex[key]
	declared := kind != ""
	if kind == "" {
		kind = hclload.KindTable
	}
	st := flowStage{
		Kind:      kind,
		KindLabel: kindLabel(kind),
		Engine:    b.engineKind[key],
		Database:  ref.Database,
		Name:      ref.Name,
		Detail:    b.detail[key],
		Declared:  declared,
		EdgeLabel: edge,
	}
	if declared {
		st.Href = objectHref(ref.Database, kind, ref.Name)
	}
	return st
}

func (b *flowBuilder) mvStage(ref hclload.ObjectRef, edge string) flowStage {
	key := indexKey(ref.Database, ref.Name)
	declared := b.kindIndex[key] != ""
	st := flowStage{
		Kind:      hclload.KindMaterializedView,
		KindLabel: kindLabel(hclload.KindMaterializedView),
		Database:  ref.Database,
		Name:      ref.Name,
		Declared:  declared,
		EdgeLabel: edge,
	}
	if declared {
		st.Href = objectHref(ref.Database, hclload.KindMaterializedView, ref.Name)
	}
	return st
}

// engineDetail returns a short subtitle for a table's engine, surfacing the
// fields most useful when reading an ingestion chain.
func engineDetail(spec *hclload.EngineSpec) string {
	if spec == nil || spec.Decoded == nil {
		return ""
	}
	switch e := spec.Decoded.(type) {
	case hclload.EngineKafka:
		if e.Collection != nil && *e.Collection != "" {
			return "collection=" + *e.Collection
		}
		if e.TopicList != nil && *e.TopicList != "" {
			return "topic=" + *e.TopicList
		}
	case hclload.EngineDistributed:
		if e.ClusterName != "" {
			return "cluster=" + e.ClusterName
		}
	}
	return ""
}

func (s *webServer) handleFlows(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/flows" {
		s.notFound(w)
		return
	}
	s.render(w, s.tmplFlows, flowsData{Title: "Data flows", Flows: s.flows, GlobalProblems: s.globalProblems})
}

// refOfKey reverses indexKey for the root lookup.
func refOfKey(key string) hclload.ObjectRef {
	for i := 0; i < len(key); i++ {
		if key[i] == '\x00' {
			return hclload.ObjectRef{Database: key[:i], Name: key[i+1:]}
		}
	}
	return hclload.ObjectRef{Name: key}
}

func clone(s []flowStage) []flowStage {
	out := make([]flowStage, len(s))
	copy(out, s)
	return out
}

func cloneSet(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
