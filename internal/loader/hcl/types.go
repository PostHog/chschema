package hcl

import "github.com/hashicorp/hcl/v2"

type DatabaseSpec struct {
	Name    string           `hcl:"name,label"`
	Tables  []TableSpec      `hcl:"table,block"`
	Patches []PatchTableSpec `hcl:"patch_table,block" diff:"-"`
}

// PatchTableSpec is a strictly additive cross-layer modification of a table.
// Only column additions are allowed; gohcl rejects any other attribute or
// block by struct shape. Consumed during resolution.
type PatchTableSpec struct {
	Name    string       `hcl:"name,label"`
	Columns []ColumnSpec `hcl:"column,block"`
}

type TableSpec struct {
	Name string `hcl:"name,label"`

	// Inheritance / control fields. Consumed during resolution; not part
	// of the schema for diff purposes.
	Extend   *string `hcl:"extend,optional"   diff:"-"`
	Abstract bool    `hcl:"abstract,optional" diff:"-"`
	Override bool    `hcl:"override,optional" diff:"-"`

	OrderBy     []string          `hcl:"order_by,optional"`
	PartitionBy *string           `hcl:"partition_by,optional"`
	SampleBy    *string           `hcl:"sample_by,optional"`
	TTL         *string           `hcl:"ttl,optional"`
	Settings    map[string]string `hcl:"settings,optional"`

	Columns []ColumnSpec `hcl:"column,block"`
	Indexes []IndexSpec  `hcl:"index,block"`
	Engine  *EngineSpec  `hcl:"engine,block"`
}

type ColumnSpec struct {
	Name string `hcl:"name,label"`
	Type string `hcl:"type"`
}

type IndexSpec struct {
	Name        string `hcl:"name,label"`
	Expr        string `hcl:"expr"`
	Type        string `hcl:"type"`
	Granularity int    `hcl:"granularity,optional"`
}

// EngineSpec holds both the raw HCL representation (Kind label + Body) and
// the typed Decoded value. Decoded is populated by ParseFile after the
// initial HCL decode. Diff comparisons use Decoded only — Kind and Body are
// raw artifacts skipped via the diff tag.
type EngineSpec struct {
	Kind string   `hcl:"kind,label"   diff:"-"`
	Body hcl.Body `hcl:",remain"      diff:"-"`

	// Decoded is populated post-parse via DecodeEngine; gohcl ignores it
	// because it has no hcl tag.
	Decoded Engine
}
