package hcl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// GeneratedSQL holds the result of GenerateSQL: the safe DDL statements ready
// to execute, plus a list of unsafe changes (engine, order_by, partition_by,
// sample_by) that ClickHouse cannot apply in place and require manual table
// recreation.
type GeneratedSQL struct {
	Statements []string
	Unsafe     []UnsafeChange
}

// UnsafeChange describes a diff entry that can't be expressed as an ALTER.
// Database and Table identify the target; Reason is a human-readable
// explanation of what would need to change.
type UnsafeChange struct {
	Database string
	Table    string
	Reason   string
}

// GenerateSQL turns a ChangeSet into ClickHouse DDL. Statements are ordered:
// CREATE TABLE, CREATE MATERIALIZED VIEW, ALTER TABLE, ALTER ... MODIFY QUERY,
// DROP VIEW, DROP TABLE — so materialized views are created after their
// destination table and dropped before it. Within the CREATE TABLE phase,
// tables are ordered by dependency so a Distributed table comes after the
// local table it forwards to; the DROP TABLE phase uses the reverse order.
// Unsafe changes (engine swap, ORDER BY change, materialized view recreation,
// etc.) are collected into Unsafe; the generator does not synthesize a
// recreate-and-swap procedure.
func GenerateSQL(cs ChangeSet) GeneratedSQL {
	var out GeneratedSQL

	// 1. Named-collection recreates (DROP+CREATE adjacent, at the FRONT
	// before any other create — dependent tables can rely on the new NC).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate && ncc.Add != nil {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
		if ncc.Error != "" {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: "",
				Table:    ncc.Name,
				Reason:   "named collection: " + ncc.Error,
			})
		}
	}

	// 2. Fresh NC adds.
	for _, ncc := range cs.NamedCollections {
		if ncc.Add != nil && !ncc.Recreate {
			out.Statements = append(out.Statements, createNamedCollectionSQL(*ncc.Add))
		}
	}

	for _, dt := range orderTablesByDependency(gatherTables(cs, addTablesOf), false) {
		out.Statements = append(out.Statements, createTableSQL(dt.Database, dt.Table))
	}
	for _, dc := range cs.Databases {
		for _, mv := range dc.AddMaterializedViews {
			out.Statements = append(out.Statements, createMaterializedViewSQL(dc.Database, mv))
		}
	}
	for _, dc := range cs.Databases {
		for _, d := range dictionariesByName(dc.AddDictionaries) {
			out.Statements = append(out.Statements, createDictionarySQL(dc.Database, d))
		}
	}
	for _, dc := range cs.Databases {
		for _, td := range dc.AlterTables {
			if td.IsUnsafe() {
				out.Unsafe = append(out.Unsafe, unsafeReasons(dc.Database, td)...)
			}
			if stmt := alterTableSQL(dc.Database, td); stmt != "" {
				out.Statements = append(out.Statements, stmt)
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, mvd := range dc.AlterMaterializedViews {
			if mvd.Recreate {
				out.Unsafe = append(out.Unsafe, UnsafeChange{
					Database: dc.Database, Table: mvd.Name,
					Reason: "materialized view to_table or column list change requires recreating the view",
				})
				continue
			}
			if mvd.QueryChange != nil && mvd.QueryChange.New != nil {
				out.Statements = append(out.Statements, modifyQuerySQL(dc.Database, mvd.Name, *mvd.QueryChange.New))
			}
		}
	}
	for _, dc := range cs.Databases {
		for _, dd := range dc.AlterDictionaries {
			out.Unsafe = append(out.Unsafe, UnsafeChange{
				Database: dc.Database,
				Table:    dd.Name,
				Reason:   fmt.Sprintf("dictionary change requires CREATE OR REPLACE DICTIONARY (changed: %s)", strings.Join(dd.Changed, ", ")),
			})
		}
	}

	// 8. ALTER NAMED COLLECTION (SET then DELETE, only for non-recreate diffs).
	for _, ncc := range cs.NamedCollections {
		if ncc.Recreate || ncc.Add != nil || ncc.Drop {
			continue
		}
		if stmt := alterNamedCollectionSetSQL(ncc.Name, ncc.SetParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
		if stmt := alterNamedCollectionDeleteSQL(ncc.Name, ncc.DeleteParams); stmt != "" {
			out.Statements = append(out.Statements, stmt)
		}
	}

	for _, dc := range cs.Databases {
		for _, name := range dc.DropMaterializedViews {
			out.Statements = append(out.Statements, dropViewSQL(dc.Database, name))
		}
	}
	for _, dc := range cs.Databases {
		names := append([]string(nil), dc.DropDictionaries...)
		sort.Strings(names)
		for _, name := range names {
			out.Statements = append(out.Statements, dropDictionarySQL(dc.Database, name))
		}
	}
	for _, dt := range orderTablesByDependency(gatherTables(cs, dropTablesOf), true) {
		out.Statements = append(out.Statements, dropTableSQL(dt.Database, dt.Table.Name))
	}

	// 12. NC pure drops (not recreate). After tables — anything referencing them is gone.
	for _, ncc := range cs.NamedCollections {
		if ncc.Drop && !ncc.Recreate {
			out.Statements = append(out.Statements, dropNamedCollectionSQL(ncc.Name))
		}
	}
	return out
}

// dbTable pairs a table with its database, so a flat slice can carry tables
// drawn from every database in a ChangeSet.
type dbTable struct {
	Database string
	Table    TableSpec
}

func addTablesOf(dc DatabaseChange) []TableSpec  { return dc.AddTables }
func dropTablesOf(dc DatabaseChange) []TableSpec { return dc.DropTables }

// gatherTables flattens one table collection (adds or drops) across every
// database in the ChangeSet into a single ordered slice.
func gatherTables(cs ChangeSet, pick func(DatabaseChange) []TableSpec) []dbTable {
	var out []dbTable
	for _, dc := range cs.Databases {
		for _, t := range pick(dc) {
			out = append(out, dbTable{Database: dc.Database, Table: t})
		}
	}
	return out
}

// orderTablesByDependency orders tables so a Distributed table comes after the
// local table it forwards to, when that table is part of the same set.
// Dependencies on tables outside the set impose no constraint. When reverse is
// true the order is flipped, so dependents come before their dependencies —
// used for DROP statements. Tables in a dependency cycle keep their input
// order. The sort is otherwise stable.
func orderTablesByDependency(tables []dbTable, reverse bool) []dbTable {
	index := make(map[ObjectRef]int, len(tables))
	for i, dt := range tables {
		index[ObjectRef{Database: dt.Database, Name: dt.Table.Name}] = i
	}

	indegree := make([]int, len(tables))     // count of unresolved dependencies
	dependents := make([][]int, len(tables)) // dependents[j] = tables that depend on j
	for i, dt := range tables {
		if dt.Table.Engine == nil {
			continue
		}
		dist, ok := dt.Table.Engine.Decoded.(EngineDistributed)
		if !ok {
			continue
		}
		remote := ObjectRef{Database: dist.RemoteDatabase, Name: dist.RemoteTable}
		if j, ok := index[remote]; ok && j != i {
			indegree[i]++
			dependents[j] = append(dependents[j], i)
		}
	}

	// Kahn's algorithm; ready nodes are emitted in input order for stability.
	done := make([]bool, len(tables))
	order := make([]int, 0, len(tables))
	for len(order) < len(tables) {
		progressed := false
		for i := range tables {
			if done[i] || indegree[i] != 0 {
				continue
			}
			order = append(order, i)
			done[i] = true
			progressed = true
			for _, d := range dependents[i] {
				indegree[d]--
			}
		}
		if !progressed { // dependency cycle: emit the rest in input order
			for i := range tables {
				if !done[i] {
					order = append(order, i)
					done[i] = true
				}
			}
		}
	}

	out := make([]dbTable, len(tables))
	for pos, i := range order {
		if reverse {
			out[len(out)-1-pos] = tables[i]
		} else {
			out[pos] = tables[i]
		}
	}
	return out
}

// createMaterializedViewSQL renders a CREATE MATERIALIZED VIEW in its
// `TO <table>` form. The column list, when present, is emitted between the
// destination table and AS — matching ClickHouse's accepted syntax.
func createMaterializedViewSQL(database string, mv MaterializedViewSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE MATERIALIZED VIEW %s.%s", database, mv.Name)
	if mv.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *mv.Cluster)
	}
	fmt.Fprintf(&b, " TO %s", mv.ToTable)
	if len(mv.Columns) > 0 {
		parts := make([]string, len(mv.Columns))
		for i, c := range mv.Columns {
			parts[i] = columnDefSQL(c)
		}
		fmt.Fprintf(&b, " (%s)", strings.Join(parts, ", "))
	}
	fmt.Fprintf(&b, " AS %s", mv.Query)
	// COMMENT comes last, after AS SELECT, per the CREATE MATERIALIZED VIEW grammar.
	if mv.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*mv.Comment))
	}
	return b.String()
}

// modifyQuerySQL renders an in-place query update for a materialized view.
func modifyQuerySQL(database, name, query string) string {
	return fmt.Sprintf("ALTER TABLE %s.%s MODIFY QUERY %s", database, name, query)
}

func dropViewSQL(database, name string) string {
	return fmt.Sprintf("DROP VIEW %s.%s", database, name)
}

func createTableSQL(database string, t TableSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s.%s", database, t.Name)
	if t.Cluster != nil {
		fmt.Fprintf(&b, " ON CLUSTER %s", *t.Cluster)
	}
	b.WriteString(" (\n")

	var parts []string
	for _, c := range t.Columns {
		parts = append(parts, "  "+columnDefSQL(c))
	}
	for _, con := range t.Constraints {
		parts = append(parts, "  "+constraintClause(con))
	}
	for _, idx := range t.Indexes {
		parts = append(parts, fmt.Sprintf("  INDEX %s", indexClause(idx)))
	}
	b.WriteString(strings.Join(parts, ",\n"))
	b.WriteString("\n)")

	clause, extraSettings := engineSQL(engineOf(t))
	fmt.Fprintf(&b, " ENGINE = %s", clause)

	if len(t.PrimaryKey) > 0 {
		fmt.Fprintf(&b, " PRIMARY KEY (%s)", strings.Join(t.PrimaryKey, ", "))
	}
	if len(t.OrderBy) > 0 {
		fmt.Fprintf(&b, " ORDER BY (%s)", strings.Join(t.OrderBy, ", "))
	}
	if t.PartitionBy != nil {
		fmt.Fprintf(&b, " PARTITION BY %s", *t.PartitionBy)
	}
	if t.SampleBy != nil {
		fmt.Fprintf(&b, " SAMPLE BY %s", *t.SampleBy)
	}
	if t.TTL != nil {
		fmt.Fprintf(&b, " TTL %s", *t.TTL)
	}

	settings := mergeSettings(t.Settings, extraSettings)
	if len(settings) > 0 {
		fmt.Fprintf(&b, " SETTINGS %s", formatSettingsList(settings))
	}

	// COMMENT must come after all storage clauses (per docs).
	if t.Comment != nil {
		fmt.Fprintf(&b, " COMMENT %s", quoteString(*t.Comment))
	}
	return b.String()
}

func constraintClause(c ConstraintSpec) string {
	if c.Check != nil {
		return fmt.Sprintf("CONSTRAINT %s CHECK %s", c.Name, *c.Check)
	}
	if c.Assume != nil {
		return fmt.Sprintf("CONSTRAINT %s ASSUME %s", c.Name, *c.Assume)
	}
	return fmt.Sprintf("CONSTRAINT %s", c.Name) // shouldn't happen post-validation
}

func dropTableSQL(database, table string) string {
	return fmt.Sprintf("DROP TABLE %s.%s", database, table)
}

func alterTableSQL(database string, td TableDiff) string {
	var ops []string
	for _, r := range td.RenameColumns {
		ops = append(ops, fmt.Sprintf("RENAME COLUMN %s TO %s", r.Old, r.New))
	}
	for _, c := range td.AddColumns {
		ops = append(ops, fmt.Sprintf("ADD COLUMN %s %s", c.Name, c.Type))
	}
	for _, n := range td.DropColumns {
		ops = append(ops, fmt.Sprintf("DROP COLUMN %s", n))
	}
	for _, c := range td.ModifyColumns {
		ops = append(ops, fmt.Sprintf("MODIFY COLUMN %s %s", c.Name, c.NewType))
	}
	for _, n := range td.DropIndexes {
		ops = append(ops, fmt.Sprintf("DROP INDEX %s", n))
	}
	for _, idx := range td.AddIndexes {
		ops = append(ops, fmt.Sprintf("ADD INDEX %s", indexClause(idx)))
	}
	for _, k := range sortedKeys(td.SettingsAdded) {
		ops = append(ops, fmt.Sprintf("MODIFY SETTING %s = %s", k, formatSettingValue(td.SettingsAdded[k])))
	}
	for _, c := range td.SettingsChanged {
		ops = append(ops, fmt.Sprintf("MODIFY SETTING %s = %s", c.Key, formatSettingValue(c.NewValue)))
	}
	for _, k := range td.SettingsRemoved {
		ops = append(ops, fmt.Sprintf("RESET SETTING %s", k))
	}
	if td.TTLChange != nil {
		if td.TTLChange.New != nil {
			ops = append(ops, fmt.Sprintf("MODIFY TTL %s", *td.TTLChange.New))
		} else {
			ops = append(ops, "REMOVE TTL")
		}
	}
	if len(ops) == 0 {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s.%s %s", database, td.Table, strings.Join(ops, ", "))
}

// columnDefSQL renders one column definition in the order ClickHouse
// documents:
//
//	name type [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr]
//	  [COMMENT 'text'] [CODEC(...)] [TTL expr]
//
// Nullable expansion happens here: `nullable = true` wraps the type in
// Nullable(...), unless the type is already a Nullable(...).
func columnDefSQL(c ColumnSpec) string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteByte(' ')
	sb.WriteString(effectiveType(c))

	switch {
	case c.Default != nil:
		fmt.Fprintf(&sb, " DEFAULT %s", *c.Default)
	case c.Materialized != nil:
		fmt.Fprintf(&sb, " MATERIALIZED %s", *c.Materialized)
	case c.Ephemeral != nil:
		if *c.Ephemeral == "" {
			sb.WriteString(" EPHEMERAL")
		} else {
			fmt.Fprintf(&sb, " EPHEMERAL %s", *c.Ephemeral)
		}
	case c.Alias != nil:
		fmt.Fprintf(&sb, " ALIAS %s", *c.Alias)
	}

	if c.Comment != nil {
		fmt.Fprintf(&sb, " COMMENT %s", quoteString(*c.Comment))
	}
	if c.Codec != nil {
		fmt.Fprintf(&sb, " CODEC(%s)", *c.Codec)
	}
	if c.TTL != nil {
		fmt.Fprintf(&sb, " TTL %s", *c.TTL)
	}
	return sb.String()
}

// effectiveType returns Type wrapped in Nullable(...) when c.Nullable is set
// and Type isn't already Nullable. The conflict case (nullable = true with a
// pre-wrapped Type) is rejected by the resolver, not here.
func effectiveType(c ColumnSpec) string {
	if c.Nullable && !strings.HasPrefix(c.Type, "Nullable(") {
		return "Nullable(" + c.Type + ")"
	}
	return c.Type
}

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "\\'") + "'"
}

func indexClause(idx IndexSpec) string {
	if idx.Granularity > 0 {
		return fmt.Sprintf("%s %s TYPE %s GRANULARITY %d", idx.Name, idx.Expr, idx.Type, idx.Granularity)
	}
	return fmt.Sprintf("%s %s TYPE %s", idx.Name, idx.Expr, idx.Type)
}

// engineOf returns the decoded Engine value from a TableSpec, or nil if the
// table has no engine (only abstract tables should ever satisfy that, and
// they don't reach SQL generation).
func engineOf(t TableSpec) Engine {
	if t.Engine == nil {
		return nil
	}
	return t.Engine.Decoded
}

// engineSQL renders an Engine as a ClickHouse engine clause. The second
// return is any extra SETTINGS that should be folded into the CREATE TABLE
// SETTINGS clause (used by Kafka, which expresses its arguments via SETTINGS
// rather than constructor args).
func engineSQL(e Engine) (clause string, extraSettings map[string]string) {
	switch v := e.(type) {
	case EngineMergeTree:
		return "MergeTree()", nil
	case EngineReplicatedMergeTree:
		return fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineReplacingMergeTree:
		if v.VersionColumn != nil {
			return fmt.Sprintf("ReplacingMergeTree(%s)", *v.VersionColumn), nil
		}
		return "ReplacingMergeTree()", nil
	case EngineReplicatedReplacingMergeTree:
		if v.VersionColumn != nil {
			return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s', %s)", v.ZooPath, v.ReplicaName, *v.VersionColumn), nil
		}
		return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineSummingMergeTree:
		if len(v.SumColumns) > 0 {
			return fmt.Sprintf("SummingMergeTree((%s))", strings.Join(v.SumColumns, ", ")), nil
		}
		return "SummingMergeTree()", nil
	case EngineCollapsingMergeTree:
		return fmt.Sprintf("CollapsingMergeTree(%s)", v.SignColumn), nil
	case EngineReplicatedCollapsingMergeTree:
		return fmt.Sprintf("ReplicatedCollapsingMergeTree('%s', '%s', %s)", v.ZooPath, v.ReplicaName, v.SignColumn), nil
	case EngineAggregatingMergeTree:
		return "AggregatingMergeTree()", nil
	case EngineReplicatedAggregatingMergeTree:
		return fmt.Sprintf("ReplicatedAggregatingMergeTree('%s', '%s')", v.ZooPath, v.ReplicaName), nil
	case EngineDistributed:
		if v.ShardingKey != nil {
			return fmt.Sprintf("Distributed('%s', '%s', '%s', %s)", v.ClusterName, v.RemoteDatabase, v.RemoteTable, *v.ShardingKey), nil
		}
		return fmt.Sprintf("Distributed('%s', '%s', '%s')", v.ClusterName, v.RemoteDatabase, v.RemoteTable), nil
	case EngineLog:
		return "Log()", nil
	case EngineKafka:
		if v.Collection != nil {
			// Named collection form: Kafka(<collection>); no settings emitted.
			return fmt.Sprintf("Kafka(%s)", *v.Collection), nil
		}
		settings := map[string]string{}
		setStr := func(name string, p *string) {
			if p != nil {
				settings[name] = *p
			}
		}
		setInt := func(name string, p *int64) {
			if p != nil {
				settings[name] = fmt.Sprintf("%d", *p)
			}
		}
		setBool := func(name string, p *bool) {
			if p != nil {
				if *p {
					settings[name] = "1"
				} else {
					settings[name] = "0"
				}
			}
		}
		setStr("kafka_broker_list", v.BrokerList)
		setStr("kafka_topic_list", v.TopicList)
		setStr("kafka_group_name", v.GroupName)
		setStr("kafka_format", v.Format)
		setStr("kafka_security_protocol", v.SecurityProtocol)
		setStr("kafka_sasl_mechanism", v.SaslMechanism)
		setStr("kafka_sasl_username", v.SaslUsername)
		setStr("kafka_sasl_password", v.SaslPassword)
		setStr("kafka_client_id", v.ClientID)
		setStr("kafka_schema", v.Schema)
		setStr("kafka_handle_error_mode", v.HandleErrorMode)
		setStr("kafka_compression_codec", v.CompressionCodec)
		setInt("kafka_num_consumers", v.NumConsumers)
		setInt("kafka_max_block_size", v.MaxBlockSize)
		setInt("kafka_skip_broken_messages", v.SkipBrokenMessages)
		setInt("kafka_poll_timeout_ms", v.PollTimeoutMs)
		setInt("kafka_poll_max_batch_size", v.PollMaxBatchSize)
		setInt("kafka_flush_interval_ms", v.FlushIntervalMs)
		setInt("kafka_consumer_reschedule_ms", v.ConsumerRescheduleMs)
		setInt("kafka_max_rows_per_message", v.MaxRowsPerMessage)
		setInt("kafka_compression_level", v.CompressionLevel)
		setBool("kafka_commit_every_batch", v.CommitEveryBatch)
		setBool("kafka_thread_per_consumer", v.ThreadPerConsumer)
		setBool("kafka_commit_on_select", v.CommitOnSelect)
		setBool("kafka_autodetect_client_rack", v.AutodetectClientRack)
		for k, val := range v.Extra {
			settings[k] = val
		}
		return "Kafka()", settings
	}
	return "", nil
}

func mergeSettings(user, extra map[string]string) map[string]string {
	if len(user) == 0 && len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(user)+len(extra))
	for k, v := range extra {
		out[k] = v
	}
	for k, v := range user {
		out[k] = v
	}
	return out
}

func formatSettingsList(settings map[string]string) string {
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s = %s", k, formatSettingValue(settings[k]))
	}
	return strings.Join(parts, ", ")
}

var numericRe = regexp.MustCompile(`^-?\d+(\.\d+)?$`)

func formatSettingValue(v string) string {
	if numericRe.MatchString(v) {
		return v
	}
	return "'" + strings.ReplaceAll(v, "'", "\\'") + "'"
}

func unsafeReasons(database string, td TableDiff) []UnsafeChange {
	var out []UnsafeChange
	if td.EngineChange != nil {
		fromKind, toKind := "(none)", "(none)"
		if td.EngineChange.Old != nil {
			fromKind = td.EngineChange.Old.Kind()
		}
		if td.EngineChange.New != nil {
			toKind = td.EngineChange.New.Kind()
		}
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: fmt.Sprintf("engine change from %s to %s requires recreating the table", fromKind, toKind),
		})
	}
	if td.OrderByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "ORDER BY change requires recreating the table",
		})
	}
	if td.PartitionByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "PARTITION BY change requires recreating the table",
		})
	}
	if td.SampleByChange != nil {
		out = append(out, UnsafeChange{
			Database: database, Table: td.Table,
			Reason: "SAMPLE BY change requires recreating the table",
		})
	}
	return out
}
