package hcl

import (
	"sort"
	"strings"
)

// RoleDiff pairs a node role with its desired and current schema. Desired is
// the composed single-role authored schema (the target); Current is the
// matching node from a topology dump (one representative per role+shard,
// replicas collapsed). The plan emits the migration that brings Current to
// Desired.
type RoleDiff struct {
	Role    string
	Desired *Schema
	Current *Schema
}

// PlanOperation is one globally-ordered operation across all roles, with role
// provenance. It extends the diff -format json operation shape (#64) with the
// roles that contribute it; identical statements across roles collapse to one
// operation carrying the union of roles.
type PlanOperation struct {
	Order        int      `json:"order"`
	Kind         string   `json:"kind"`
	ObjectType   string   `json:"object_type"`
	Database     string   `json:"database"`
	Object       string   `json:"object"`
	Engine       string   `json:"engine"`
	Replicated   bool     `json:"replicated"`
	SQL          string   `json:"sql"`
	Manual       bool     `json:"manual"` // operator-run only (e.g. MATERIALIZE INDEX); executors must skip it
	Roles        []string `json:"roles"`
	Unsafe       bool     `json:"unsafe"`
	UnsafeReason string   `json:"unsafe_reason"`
}

// RoleComparison is one role's per-object view of its diff. Unlike the
// merged Operations list it is deliberately NOT deduped across roles —
// triage is per (env, role), so a shared object drifting on two roles
// appears under both.
type RoleComparison struct {
	Role    string             `json:"role"`
	Objects []ObjectComparison `json:"objects"`
	Summary CompareSummary     `json:"summary"`
}

// PlanResult is the merged, globally-ordered plan across every role.
type PlanResult struct {
	Operations []PlanOperation  `json:"operations"`
	Unsafe     []JSONUnsafe     `json:"unsafe,omitempty"`
	Roles      []RoleComparison `json:"roles"`
}

// BuildPlan diffs every role, unions the per-role operations into one global
// operation list, and orders it by a cross-role dependency graph built over the
// unioned desired schema. Because every role's objects share one
// (database, object) keyspace, a Distributed/Buffer object that forwards to a
// table physically hosted on another role (e.g. a per-role
// writable_query_log_archive -> the OPS sharded_query_log_archive) gets a real
// dependency edge — the cross-role ordering the issue is about.
//
// Ordering: CREATE and widening ALTER follow dependency direction (a referenced
// object before the object that references it — storage before proxies before
// the MV); DROP runs in reverse. Identical statements across roles dedupe to a
// single operation carrying the union of contributing roles.
func BuildPlan(roles []RoleDiff) PlanResult {
	type opKey struct{ kind, db, object, sql string }

	var firstSeen []opKey
	byKey := make(map[opKey]*PlanOperation)
	unsafeByRef := make(map[ObjectRef]string)

	// Non-nil so an empty plan marshals roles as [], not null (same contract
	// as DiffJSON.Objects).
	roleComparisons := make([]RoleComparison, 0, len(roles))
	for _, rd := range roles {
		cs := Diff(rd.Current, rd.Desired)
		gen := GenerateSQL(cs)
		objs := BuildObjectComparisons(cs, gen, rd.Current, rd.Desired)
		roleComparisons = append(roleComparisons, RoleComparison{
			Role: rd.Role, Objects: objs, Summary: SummarizeComparisons(objs),
		})
		for _, op := range gen.Ops {
			k := opKey{op.Kind, op.Database, op.Object, op.SQL}
			po, ok := byKey[k]
			if !ok {
				po = &PlanOperation{
					Kind:       op.Kind,
					ObjectType: op.ObjectType,
					Database:   op.Database,
					Object:     op.Object,
					SQL:        op.SQL,
					Manual:     op.Manual,
				}
				byKey[k] = po
				firstSeen = append(firstSeen, k)
			}
			po.Roles = appendUniqueRole(po.Roles, rd.Role)
		}
		for _, u := range gen.Unsafe {
			unsafeByRef[ObjectRef{Database: u.Database, Name: u.Table}] = u.Reason
		}
	}

	merged := mergeDesiredSchemas(roles)
	rank := dependencyRank(merged.Databases)

	ops := make([]*PlanOperation, 0, len(firstSeen))
	for _, k := range firstSeen {
		po := byKey[k]
		if po.ObjectType == KindTable {
			po.Engine = engineFor(po.Database, po.Object, merged)
			po.Replicated = strings.HasPrefix(po.Engine, "Replicated")
		}
		if reason, ok := unsafeByRef[ObjectRef{Database: po.Database, Name: po.Object}]; ok {
			po.Unsafe = true
			po.UnsafeReason = reason
		}
		ops = append(ops, po)
	}

	// Phase first (CREATE -> ALTER -> DROP), then dependency rank within a
	// phase: ascending for CREATE/ALTER (dependency before dependent), reverse
	// for DROP. SliceStable keeps the deterministic first-seen order for ties
	// (independent objects with equal rank).
	sort.SliceStable(ops, func(i, j int) bool {
		pi, pj := planPhase(ops[i].Kind), planPhase(ops[j].Kind)
		if pi != pj {
			return pi < pj
		}
		ri := rank[ObjectRef{Database: ops[i].Database, Name: ops[i].Object}]
		rj := rank[ObjectRef{Database: ops[j].Database, Name: ops[j].Object}]
		if ri == rj {
			return false
		}
		if ops[i].Kind == OpDrop {
			return ri > rj
		}
		return ri < rj
	})

	result := PlanResult{Operations: make([]PlanOperation, 0, len(ops))}
	for i, po := range ops {
		po.Order = i
		result.Operations = append(result.Operations, *po)
	}
	for ref, reason := range unsafeByRef {
		result.Unsafe = append(result.Unsafe, JSONUnsafe{Database: ref.Database, Object: ref.Name, Reason: reason})
	}
	sort.Slice(result.Unsafe, func(i, j int) bool {
		if result.Unsafe[i].Database != result.Unsafe[j].Database {
			return result.Unsafe[i].Database < result.Unsafe[j].Database
		}
		return result.Unsafe[i].Object < result.Unsafe[j].Object
	})

	// Each role's ops were numbered against that role's own diff. Renumber them
	// to the merged global order so an object's nested operations and the flat
	// execution list agree on sequencing.
	orderByKey := map[opKey]int{}
	for _, po := range result.Operations {
		orderByKey[opKey{po.Kind, po.Database, po.Object, po.SQL}] = po.Order
	}
	for ri := range roleComparisons {
		for oi := range roleComparisons[ri].Objects {
			ops := roleComparisons[ri].Objects[oi].Operations
			for pi := range ops {
				ops[pi].Order = orderByKey[opKey{ops[pi].Kind, ops[pi].Database, ops[pi].Object, ops[pi].SQL}]
			}
		}
	}
	result.Roles = roleComparisons
	return result
}

// planPhase groups operation kinds so CREATEs precede ALTERs precede DROPs.
// RENAME is ordered with ALTERs.
func planPhase(kind string) int {
	switch kind {
	case OpCreate:
		return 0
	case OpDrop:
		return 2
	default: // OpAlter, OpRename
		return 1
	}
}

// dependencyRank topologically ranks every object in the unioned desired
// schema: a referenced object (e.g. a storage table) gets a lower rank than the
// objects that reference it (Buffer/Distributed proxies, then the MV). Objects
// in a dependency cycle, or absent from the graph, fall back to input order.
func dependencyRank(dbs []DatabaseSpec) map[ObjectRef]int {
	var nodes []createNode
	for _, db := range dbs {
		for _, t := range db.Tables {
			nodes = append(nodes, createNode{ref: ObjectRef{Database: db.Name, Name: t.Name}})
		}
		for _, mv := range db.MaterializedViews {
			nodes = append(nodes, createNode{ref: ObjectRef{Database: db.Name, Name: mv.Name}})
		}
		for _, v := range db.Views {
			nodes = append(nodes, createNode{ref: ObjectRef{Database: db.Name, Name: v.Name}})
		}
		for _, d := range db.Dictionaries {
			nodes = append(nodes, createNode{ref: ObjectRef{Database: db.Name, Name: d.Name}})
		}
	}

	var edges [][2]ObjectRef
	if deps, err := CollectDependencies(dbs); err == nil {
		for _, d := range deps {
			edges = append(edges, [2]ObjectRef{d.From, d.To})
		}
	}

	rank := make(map[ObjectRef]int, len(nodes))
	for i, n := range topoSortNodes(nodes, edges) {
		rank[n.ref] = i
	}
	return rank
}

// mergeDesiredSchemas unions every role's desired (left) schema into one,
// deduping objects by (database, name) — first role wins. The union is the
// keyspace the cross-role dependency graph is built over.
func mergeDesiredSchemas(roles []RoleDiff) *Schema {
	merged := &Schema{}
	dbIndex := make(map[string]int)
	getDB := func(name string) *DatabaseSpec {
		if i, ok := dbIndex[name]; ok {
			return &merged.Databases[i]
		}
		merged.Databases = append(merged.Databases, DatabaseSpec{Name: name})
		dbIndex[name] = len(merged.Databases) - 1
		return &merged.Databases[len(merged.Databases)-1]
	}

	seen := make(map[ObjectRef]bool)
	mark := func(db, name string) bool {
		ref := ObjectRef{Database: db, Name: name}
		if seen[ref] {
			return false
		}
		seen[ref] = true
		return true
	}

	for _, rd := range roles {
		if rd.Desired == nil {
			continue
		}
		for _, db := range rd.Desired.Databases {
			d := getDB(db.Name)
			for _, t := range db.Tables {
				if mark(db.Name, t.Name) {
					d.Tables = append(d.Tables, t)
				}
			}
			for _, mv := range db.MaterializedViews {
				if mark(db.Name, mv.Name) {
					d.MaterializedViews = append(d.MaterializedViews, mv)
				}
			}
			for _, v := range db.Views {
				if mark(db.Name, v.Name) {
					d.Views = append(d.Views, v)
				}
			}
			for _, dct := range db.Dictionaries {
				if mark(db.Name, dct.Name) {
					d.Dictionaries = append(d.Dictionaries, dct)
				}
			}
			// A raw object's identity is (kind, name), so key it separately —
			// two raw kinds may legally share a name (see indexRaws in diff.go).
			for _, r := range db.Raws {
				if mark(db.Name, r.Kind+"\x00"+r.Name) {
					d.Raws = append(d.Raws, r)
				}
			}
		}
	}
	return merged
}

func appendUniqueRole(roles []string, role string) []string {
	for _, r := range roles {
		if r == role {
			return roles
		}
	}
	return append(roles, role)
}
