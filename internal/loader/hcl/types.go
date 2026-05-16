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

	// MaterializedViews are a sibling collection to Tables, not a flavored
	// TableSpec. MVs do not participate in the table inheritance system
	// (extend / abstract / patch_table).
	MaterializedViews []MaterializedViewSpec `hcl:"materialized_view,block"`

	// Dictionaries are a third sibling collection. Like MVs, they do not
	// participate in the table inheritance system.
	Dictionaries []DictionarySpec `hcl:"dictionary,block"`
}

// MaterializedViewSpec models a ClickHouse materialized view in its
// `TO <table>` form: the MV reads from its source (referenced in Query) and
// writes rows into an existing destination table. Inner-engine MVs
// (ENGINE = ...), refreshable MVs (REFRESH ...), and window views are not
// supported and are rejected with a clear error during introspection.
type MaterializedViewSpec struct {
	Name    string       `hcl:"name,label"`
	ToTable string       `hcl:"to_table"`         // TO <db.>table target (required)
	Columns []ColumnSpec `hcl:"column,block"`     // explicit column list (may be empty)
	Query   string       `hcl:"query"`            // the AS SELECT ... body
	Cluster *string      `hcl:"cluster,optional"` // ON CLUSTER
	Comment *string      `hcl:"comment,optional"`
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

// DictionarySpec models a ClickHouse dictionary. Dictionaries are a sibling
// collection on DatabaseSpec, not a flavored table; they do not participate
// in the table inheritance system (extend/abstract/patch_table). Source and
// layout are modeled typed-per-kind via DictionarySource/DictionaryLayout —
// the same pattern engines use.
type DictionarySpec struct {
	Name       string                `hcl:"name,label"`
	PrimaryKey []string              `hcl:"primary_key"`
	Attributes []DictionaryAttribute `hcl:"attribute,block"`
	Source     *DictionarySourceSpec `hcl:"source,block"`
	Layout     *DictionaryLayoutSpec `hcl:"layout,block"`
	Lifetime   *DictionaryLifetime   `hcl:"lifetime,block"`
	Range      *DictionaryRange      `hcl:"range,block"`
	Settings   map[string]string     `hcl:"settings,optional"`
	Cluster    *string               `hcl:"cluster,optional"`
	Comment    *string               `hcl:"comment,optional"`
}

type DictionaryAttribute struct {
	Name         string  `hcl:"name,label"`
	Type         string  `hcl:"type"`
	Default      *string `hcl:"default,optional"`
	Expression   *string `hcl:"expression,optional"`
	Hierarchical bool    `hcl:"hierarchical,optional"`
	Injective    bool    `hcl:"injective,optional"`
	IsObjectID   bool    `hcl:"is_object_id,optional"`
}

// DictionaryLifetime models LIFETIME(...). The simple form LIFETIME(n) sets
// only Min (Max remains nil). The range form LIFETIME(MIN x MAX y) sets both.
type DictionaryLifetime struct {
	Min *int64 `hcl:"min,optional"`
	Max *int64 `hcl:"max,optional"`
}

type DictionaryRange struct {
	Min string `hcl:"min"`
	Max string `hcl:"max"`
}

// DictionarySourceSpec mirrors EngineSpec: Kind label + opaque hcl.Body +
// typed Decoded value. Decoded is populated by DecodeDictionarySource after
// the initial gohcl decode; Kind and Body are diff-skipped artifacts.
type DictionarySourceSpec struct {
	Kind string   `hcl:"kind,label" diff:"-"`
	Body hcl.Body `hcl:",remain"    diff:"-"`

	Decoded DictionarySource
}

type DictionarySource interface{ Kind() string }

type DictionaryLayoutSpec struct {
	Kind string   `hcl:"kind,label" diff:"-"`
	Body hcl.Body `hcl:",remain"    diff:"-"`

	Decoded DictionaryLayout
}

type DictionaryLayout interface{ Kind() string }

// Schema is what ParseFile returns. It carries both top-level kinds
// hclexp tracks: databases (with their tables/MVs/dictionaries) and
// named collections (cluster-scoped, separate from any database).
type Schema struct {
	Databases        []DatabaseSpec
	NamedCollections []NamedCollectionSpec
}

// NamedCollectionSpec models a ClickHouse named collection — a
// cluster-scoped key/value bag of configuration values that other
// objects (most notably Kafka tables) can reference by name.
//
// External = true marks a collection as managed outside hclexp — for
// example, defined in the ClickHouse server XML config under
// /etc/clickhouse-server/config.d/. hclexp emits no DDL for external
// collections (no CREATE / ALTER / DROP); they exist in HCL only as
// declarations so engine `collection = "x"` references can resolve and
// be validated. Params is optional when External = true (the values
// live in the XML config, not in HCL).
type NamedCollectionSpec struct {
	Name     string                 `hcl:"name,label"`
	External bool                   `hcl:"external,optional"`
	Override bool                   `hcl:"override,optional" diff:"-"`
	Cluster  *string                `hcl:"cluster,optional"`
	Comment  *string                `hcl:"comment,optional"`
	Params   []NamedCollectionParam `hcl:"param,block"`
}

type NamedCollectionParam struct {
	Key         string `hcl:"name,label"`
	Value       string `hcl:"value"`
	Overridable *bool  `hcl:"overridable,optional"`
}
