package hcl

// Drift document types for `hclexp drift -format json`. Direction is
// descriptive: each drifter's objects/operations describe the DDL that would
// transform the group reference into that node — NOT a fix script. The
// reference is merely the lexically-first group member; authoritative
// desired state comes from `plan`/`diff` against HCL.

// DriftNode is one drifting node's comparison against its group reference.
type DriftNode struct {
	Node    string             `json:"node"`
	File    string             `json:"file"`
	Macros  map[string]string  `json:"macros,omitempty"`
	Objects []ObjectComparison `json:"objects"`
	Summary CompareSummary     `json:"summary"`
}

// DriftGroup is one set of nodes expected to share a schema.
type DriftGroup struct {
	Key       string      `json:"key"`
	Reference string      `json:"reference"`
	Nodes     int         `json:"nodes"`
	Drifters  []DriftNode `json:"drifters"`
}

// DriftRunSummary aggregates a whole drift run.
type DriftRunSummary struct {
	Nodes           int `json:"nodes"`
	Groups          int `json:"groups"`
	GroupsWithDrift int `json:"groups_with_drift"`
	DriftingNodes   int `json:"drifting_nodes"`
}

// DriftJSON is the document emitted by `hclexp drift -format json`.
type DriftJSON struct {
	Groups  []DriftGroup    `json:"groups"`
	Summary DriftRunSummary `json:"summary"`
}
