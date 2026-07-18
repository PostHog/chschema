package hcl

import (
	"strings"

	"github.com/hashicorp/hcl/v2"
)

type DatabaseSpec struct {
	Name string `hcl:"name,label"`

	// Cluster is the default ON CLUSTER target for every table declared in
	// this database. Tables can override via TableSpec.Cluster. Inheritance
	// happens during resolution; after Resolve, every TableSpec.Cluster
	// reflects the effective value.
	Cluster *string `hcl:"cluster,optional"`

	Tables  []TableSpec      `hcl:"table,block"`
	Patches []PatchTableSpec `hcl:"patch_table,block" diff:"-"`

	// ViewPatches and DictionaryPatches are the patch_view /
	// patch_dictionary counterparts of Patches. Like table patches they
	// accumulate across layers and are consumed during resolution.
	ViewPatches       []PatchViewSpec       `hcl:"patch_view,block"       diff:"-"`
	DictionaryPatches []PatchDictionarySpec `hcl:"patch_dictionary,block" diff:"-"`

	// MaterializedViews are a sibling collection to Tables, not a flavored
	// TableSpec. They may `extend` an `abstract = true` table to inherit
	// its column list (and optionally cluster/comment); `patch_table` does
	// not apply to MVs.
	MaterializedViews []MaterializedViewSpec `hcl:"materialized_view,block"`

	// Dictionaries are a third sibling collection. Like MVs, they do not
	// participate in the table inheritance system.
	Dictionaries []DictionarySpec `hcl:"dictionary,block"`

	// Views are plain (non-materialized) views. Like MVs and dictionaries
	// they live as a sibling collection and do not participate in the
	// table inheritance system.
	Views []ViewSpec `hcl:"view,block"`

	// Raws are escape-hatch objects whose CREATE DDL could not be parsed
	// or expressed in this schema language. They are stored verbatim,
	// round-tripped unchanged, and recreated (DROP+CREATE) on change.
	Raws []RawSpec `hcl:"raw,block"`
}

// RawSpec is an opaque object captured as its original CREATE DDL. It is the
// escape hatch for DDL the parser cannot handle or the HCL model cannot
// express. The two labels mirror Terraform's `resource "<type>" "<name>"`:
// Kind drives the DROP form on a recreate, Name is the object name, and SQL
// is emitted verbatim on apply.
type RawSpec struct {
	Kind string `hcl:"kind,label"` // table | materialized_view | view | dictionary
	Name string `hcl:"name,label"`
	SQL  string `hcl:"sql"`
}

// rawKinds is the set of kinds a RawSpec label may take.
var rawKinds = map[string]bool{
	"table":             true,
	"materialized_view": true,
	"view":              true,
	"dictionary":        true,
}

// normalizeRawSQL canonicalizes a raw SQL body to end with exactly one
// trailing newline. Applied both when capturing during introspection and when
// decoding from HCL, it makes the stored form independent of authoring style
// (quoted string vs heredoc) so round-tripping is exact and diffs are stable.
// Trailing newlines are never semantically significant in a CREATE statement.
func normalizeRawSQL(s string) string {
	return strings.TrimRight(s, "\n") + "\n"
}

// MaterializedViewSpec models a ClickHouse materialized view in its
// `TO <table>` form: the MV reads from its source (referenced in Query) and
// writes rows into an existing destination table. Inner-engine MVs
// (ENGINE = ...), refreshable MVs (REFRESH ...), and window views are not
// supported and are rejected with a clear error during introspection.
type MaterializedViewSpec struct {
	Name string `hcl:"name,label"`

	// Inheritance fields. Consumed during resolution; not part of the
	// schema for diff purposes. Extend names an abstract table whose
	// columns (and optionally cluster/comment) are merged into this MV.
	// Abstract MVs are dropped before diff, like abstract tables.
	Extend   *string `hcl:"extend,optional"   diff:"-"`
	Abstract bool    `hcl:"abstract,optional" diff:"-"`

	ToTable string       `hcl:"to_table,optional"` // TO <db.>table target (required when not abstract)
	Columns []ColumnSpec `hcl:"column,block"`      // explicit column list (may be empty; merged with parent on extend)
	Query   string       `hcl:"query,optional"`    // the AS SELECT ... body (required when not abstract)
	Cluster *string      `hcl:"cluster,optional"`  // ON CLUSTER
	Comment *string      `hcl:"comment,optional"`
}

// ViewSpec models a ClickHouse plain (non-materialized) view: a saved
// SELECT executed on every read of the view. The Query is stored
// verbatim as text. Live views, refreshable materialized views, and
// window views are unsupported and rejected with a clear error during
// introspection (they parse into different AST types).
//
// SQLSecurity is the canonical lowercase form of the SQL SECURITY
// clause: one of "definer", "invoker", or "none". Definer is the user
// the view runs as; required iff SQLSecurity == "definer" with a
// named user. The literal "current_user" is accepted unquoted.
type ViewSpec struct {
	Name          string   `hcl:"name,label"`
	Query         string   `hcl:"query"`
	ColumnAliases []string `hcl:"column_aliases,optional"`
	SQLSecurity   *string  `hcl:"sql_security,optional"`
	Definer       *string  `hcl:"definer,optional"`
	Cluster       *string  `hcl:"cluster,optional"`
	Comment       *string  `hcl:"comment,optional"`
}

// PatchTableSpec is a cross-layer modification of a table: the table stays
// declared once, and an env layer patches just its delta (#152, #154).
// Consumed during resolution; patches accumulate and apply in layer order,
// before extend resolution. Per-field semantics:
//
//   - Columns add (a name already on the target errors); ModifyColumns
//     replace an existing column in place (an unknown name errors);
//     DropColumns remove (unknown errors). Applied modify → drop → add,
//     so add sees the post-drop state.
//   - Indexes add, DropIndexes remove; drops apply first, so a drop+add
//     pair in one patch redefines an index.
//   - OrderBy / PartitionBy / SampleBy / TTL replace the target's value
//     when set.
//   - Engine replaces the target's engine block wholesale — merging engine
//     sub-arguments is not meaningful.
//   - Settings merge into the target's map, patch wins on key collision.
type PatchTableSpec struct {
	Name          string            `hcl:"name,label"`
	Columns       []ColumnSpec      `hcl:"column,block"`
	ModifyColumns []ColumnSpec      `hcl:"modify_column,block"`
	DropColumns   []string          `hcl:"drop_columns,optional"`
	Indexes       []IndexSpec       `hcl:"index,block"`
	DropIndexes   []string          `hcl:"drop_indexes,optional"`
	OrderBy       []string          `hcl:"order_by,optional"`
	PartitionBy   *string           `hcl:"partition_by,optional"`
	SampleBy      *string           `hcl:"sample_by,optional"`
	TTL           *string           `hcl:"ttl,optional"`
	Settings      map[string]string `hcl:"settings,optional"`
	Engine        *EngineSpec       `hcl:"engine,block"`
}

// PatchViewSpec is the view counterpart of patch_table: the view stays
// declared once and an env layer replaces just the fields it sets. Query is
// normalized to the canonical beautified form at parse, like a declared
// view's.
type PatchViewSpec struct {
	Name    string  `hcl:"name,label"`
	Query   *string `hcl:"query,optional"`
	Comment *string `hcl:"comment,optional"`
}

// PatchDictionarySpec is the dictionary counterpart of patch_table. Source,
// Layout, and Lifetime replace the target's wholesale when set; Settings
// merge patch-wins, like patch_table's.
type PatchDictionarySpec struct {
	Name     string                `hcl:"name,label"`
	Source   *DictionarySourceSpec `hcl:"source,block"`
	Layout   *DictionaryLayoutSpec `hcl:"layout,block"`
	Lifetime *DictionaryLifetime   `hcl:"lifetime,block"`
	Settings map[string]string     `hcl:"settings,optional"`
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
	Projections []ProjectionSpec `hcl:"projection,block"`
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

// ProjectionSpec is a table PROJECTION: a named SELECT (implicit FROM
// the parent table) materialized per part. Settings maps to the newer
// `WITH SETTINGS (…)` clause (projection-level MergeTree settings);
// ClickHouse before 26.5 neither emits nor accepts it.
type ProjectionSpec struct {
	Name     string            `hcl:"name,label"`
	Query    string            `hcl:"query"`
	Settings map[string]string `hcl:"settings,optional"`
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

	// Nodes carries per-node identity captured at introspection time:
	// the node hostname (label) and its ClickHouse macros (shard,
	// replica, hostClusterRole, hostClusterType, …). It is metadata only
	// — Diff() ignores it — and exists so multi-node drift analysis can
	// group nodes by their authoritative macros rather than by filename.
	Nodes []NodeSpec
}

// NodeSpec records the identity of a single physical ClickHouse node,
// dumped alongside its schema. Name is the node hostname; Macros is the
// raw key/value bag from `SELECT * FROM system.macros`.
type NodeSpec struct {
	Name   string            `hcl:"name,label"`
	Macros map[string]string `hcl:"macros,optional"`
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
