package hcl

import "github.com/hashicorp/hcl/v2"

type DatabaseSpec struct {
	Name string `hcl:"name,label"`

	// Cluster is the default ON CLUSTER target for every table declared in
	// this database. Tables can override via TableSpec.Cluster. Inheritance
	// happens during resolution; after Resolve, every TableSpec.Cluster
	// reflects the effective value.
	Cluster *string `hcl:"cluster,optional"`

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

	PrimaryKey  []string          `hcl:"primary_key,optional"`
	OrderBy     []string          `hcl:"order_by,optional"`
	PartitionBy *string           `hcl:"partition_by,optional"`
	SampleBy    *string           `hcl:"sample_by,optional"`
	TTL         *string           `hcl:"ttl,optional"`
	Settings    map[string]string `hcl:"settings,optional"`
	Comment     *string           `hcl:"comment,optional"`

	// Cluster is the ON CLUSTER target. May be set on the table itself, or
	// inherited from DatabaseSpec.Cluster during resolution.
	Cluster *string `hcl:"cluster,optional"`

	Columns     []ColumnSpec     `hcl:"column,block"`
	Indexes     []IndexSpec      `hcl:"index,block"`
	Constraints []ConstraintSpec `hcl:"constraint,block"`
	Engine      *EngineSpec      `hcl:"engine,block"`
}

// ConstraintSpec is a table-level constraint. Either Check or Assume must be
// set, but not both. Check enforces a row-level boolean predicate on INSERT;
// Assume is an optimizer hint (the constraint is presumed true).
type ConstraintSpec struct {
	Name   string  `hcl:"name,label"`
	Check  *string `hcl:"check,optional"`
	Assume *string `hcl:"assume,optional"`
}

type ColumnSpec struct {
	Name string `hcl:"name,label"`
	Type string `hcl:"type"`

	// Nullable wraps Type in Nullable(...) at SQL gen time. Combining
	// Nullable = true with a Type that already starts with "Nullable("
	// is rejected, matching ClickHouse's own behavior.
	Nullable bool `hcl:"nullable,optional"`

	// Default / Materialized / Ephemeral / Alias are mutually exclusive
	// default-value expressions, matching ClickHouse's
	// DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS clauses. Ephemeral can be the
	// empty string, meaning a bare EPHEMERAL with no expression.
	Default      *string `hcl:"default,optional"`
	Materialized *string `hcl:"materialized,optional"`
	Ephemeral    *string `hcl:"ephemeral,optional"`
	Alias        *string `hcl:"alias,optional"`

	Comment *string `hcl:"comment,optional"`
	Codec   *string `hcl:"codec,optional"`
	TTL     *string `hcl:"ttl,optional"`

	// RenamedFrom declares that this column previously existed under another
	// name. The diff engine uses it to emit RENAME COLUMN instead of DROP +
	// ADD. Tagged diff:"-" because it's transient metadata, not part of the
	// schema being compared.
	RenamedFrom *string `hcl:"renamed_from,optional" diff:"-"`
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
