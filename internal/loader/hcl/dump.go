package hcl

import (
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// Write emits dbs as canonical HCL. Tables are sorted alphabetically; columns,
// indexes, and engine fields keep their structural order. Settings entries
// are sorted by key.
//
// The dumper assumes the input has already been resolved: extend/abstract/
// override are consumed, patches applied, engines decoded. Fields tagged
// diff:"-" in the type definitions are intentionally never emitted.
func Write(w io.Writer, schema *Schema) error {
	if schema == nil {
		return errors.New("Write: nil schema")
	}
	f := hclwrite.NewEmptyFile()
	body := f.Body()

	nodes := append([]NodeSpec(nil), schema.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	for i, n := range nodes {
		if i > 0 {
			body.AppendNewline()
		}
		nodeBlock := body.AppendNewBlock("node", []string{n.Name})
		writeNode(nodeBlock.Body(), n)
	}

	for i, db := range schema.Databases {
		if i > 0 || len(nodes) > 0 {
			body.AppendNewline()
		}
		dbBlock := body.AppendNewBlock("database", []string{db.Name})
		writeDatabase(dbBlock.Body(), db)
	}

	ncs := append([]NamedCollectionSpec(nil), schema.NamedCollections...)
	sort.Slice(ncs, func(i, j int) bool { return ncs[i].Name < ncs[j].Name })
	for i, nc := range ncs {
		if len(schema.Databases) > 0 || i > 0 {
			body.AppendNewline()
		}
		ncBlock := body.AppendNewBlock("named_collection", []string{nc.Name})
		writeNamedCollection(ncBlock.Body(), nc)
	}

	_, err := w.Write(f.Bytes())
	return err
}

// writeNode emits a node block carrying the node's macros. Macro keys are
// sorted for deterministic output (via stringMap).
func writeNode(body *hclwrite.Body, n NodeSpec) {
	if len(n.Macros) > 0 {
		body.SetAttributeValue("macros", stringMap(n.Macros))
	}
}

func writeDatabase(body *hclwrite.Body, db DatabaseSpec) {
	if db.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*db.Cluster))
	}

	tables := append([]TableSpec(nil), db.Tables...)
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	for i, tbl := range tables {
		if i > 0 {
			body.AppendNewline()
		}
		tblBlock := body.AppendNewBlock("table", []string{tbl.Name})
		writeTable(tblBlock.Body(), tbl)
	}

	mvs := append([]MaterializedViewSpec(nil), db.MaterializedViews...)
	sort.Slice(mvs, func(i, j int) bool {
		return mvs[i].Name < mvs[j].Name
	})
	for i, mv := range mvs {
		if len(tables) > 0 || i > 0 {
			body.AppendNewline()
		}
		mvBlock := body.AppendNewBlock("materialized_view", []string{mv.Name})
		writeMaterializedView(mvBlock.Body(), mv)
	}

	views := append([]ViewSpec(nil), db.Views...)
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	for i, v := range views {
		if len(tables) > 0 || len(mvs) > 0 || i > 0 {
			body.AppendNewline()
		}
		vBlock := body.AppendNewBlock("view", []string{v.Name})
		writeView(vBlock.Body(), v)
	}

	dicts := append([]DictionarySpec(nil), db.Dictionaries...)
	sort.Slice(dicts, func(i, j int) bool { return dicts[i].Name < dicts[j].Name })
	for i, d := range dicts {
		if len(tables) > 0 || len(mvs) > 0 || len(views) > 0 || i > 0 {
			body.AppendNewline()
		}
		dBlock := body.AppendNewBlock("dictionary", []string{d.Name})
		writeDictionary(dBlock.Body(), d)
	}

	raws := append([]RawSpec(nil), db.Raws...)
	sort.Slice(raws, func(i, j int) bool { return raws[i].Name < raws[j].Name })
	for i, r := range raws {
		if len(tables) > 0 || len(mvs) > 0 || len(views) > 0 || len(dicts) > 0 || i > 0 {
			body.AppendNewline()
		}
		rBlock := body.AppendNewBlock("raw", []string{r.Kind, r.Name})
		writeRaw(rBlock.Body(), r)
	}
}

// writeRaw emits a raw escape-hatch block. The CREATE DDL is rendered as a
// heredoc when it spans multiple lines (the common case, for readability) and
// as a quoted string otherwise. Heredoc emission is exact: the re-parsed value
// equals the stored SQL.
func writeRaw(body *hclwrite.Body, r RawSpec) {
	setSQLAttribute(body, "sql", r.SQL)
}

// setSQLAttribute writes a (normalized, trailing-newline) SQL string. Genuinely
// multi-line bodies are emitted as a plain heredoc so the DDL stays readable in
// a dump; the heredoc body is the SQL verbatim and re-parses to the same value.
// Single-line bodies use a quoted string. Callers must pass normalizeRawSQL'd
// input so the value always ends in exactly one newline (heredocs always do).
func setSQLAttribute(body *hclwrite.Body, name, sql string) {
	if strings.Count(sql, "\n") <= 1 {
		body.SetAttributeValue(name, cty.StringVal(sql))
		return
	}
	body.SetAttributeRaw(name, hclwrite.Tokens{
		{Type: hclsyntax.TokenOHeredoc, Bytes: []byte("<<SQL\n")},
		{Type: hclsyntax.TokenStringLit, Bytes: []byte(sql)},
		{Type: hclsyntax.TokenCHeredoc, Bytes: []byte("SQL\n")},
	})
}

// setQueryAttribute writes a view/MV query, as a readable heredoc when it spans
// multiple lines (the beautified canonical form) and a quoted string otherwise.
// Multi-line bodies get a trailing newline so the heredoc terminator sits on its
// own line; the re-parsed value is re-normalized on load, so the round-trip is
// stable.
func setQueryAttribute(body *hclwrite.Body, query string) {
	if !strings.Contains(query, "\n") {
		body.SetAttributeValue("query", cty.StringVal(query))
		return
	}
	setSQLAttribute(body, "query", query+"\n")
}

func writeView(body *hclwrite.Body, v ViewSpec) {
	setQueryAttribute(body, v.Query)
	if len(v.ColumnAliases) > 0 {
		body.SetAttributeValue("column_aliases", stringList(v.ColumnAliases))
	}
	if v.SQLSecurity != nil {
		body.SetAttributeValue("sql_security", cty.StringVal(*v.SQLSecurity))
	}
	if v.Definer != nil {
		body.SetAttributeValue("definer", cty.StringVal(*v.Definer))
	}
	if v.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*v.Cluster))
	}
	if v.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*v.Comment))
	}
}

func writeMaterializedView(body *hclwrite.Body, mv MaterializedViewSpec) {
	body.SetAttributeValue("to_table", cty.StringVal(mv.ToTable))
	setQueryAttribute(body, mv.Query)
	if mv.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*mv.Cluster))
	}
	if mv.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*mv.Comment))
	}
	for _, c := range mv.Columns {
		writeColumn(body, c)
	}
}

func writeTable(body *hclwrite.Body, t TableSpec) {
	if t.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*t.Comment))
	}
	if t.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*t.Cluster))
	}
	if len(t.PrimaryKey) > 0 {
		body.SetAttributeValue("primary_key", stringList(t.PrimaryKey))
	}
	if len(t.OrderBy) > 0 {
		body.SetAttributeValue("order_by", stringList(t.OrderBy))
	}
	if t.PartitionBy != nil {
		body.SetAttributeValue("partition_by", cty.StringVal(*t.PartitionBy))
	}
	if t.SampleBy != nil {
		body.SetAttributeValue("sample_by", cty.StringVal(*t.SampleBy))
	}
	if t.TTL != nil {
		body.SetAttributeValue("ttl", cty.StringVal(*t.TTL))
	}
	if len(t.Settings) > 0 {
		body.SetAttributeValue("settings", stringMap(t.Settings))
	}

	for _, c := range t.Columns {
		writeColumn(body, c)
	}

	for _, idx := range t.Indexes {
		idxBlock := body.AppendNewBlock("index", []string{idx.Name})
		ib := idxBlock.Body()
		ib.SetAttributeValue("expr", cty.StringVal(idx.Expr))
		ib.SetAttributeValue("type", cty.StringVal(idx.Type))
		if idx.Granularity != 0 {
			ib.SetAttributeValue("granularity", cty.NumberIntVal(int64(idx.Granularity)))
		}
	}

	for _, c := range t.Constraints {
		cb := body.AppendNewBlock("constraint", []string{c.Name}).Body()
		if c.Check != nil {
			cb.SetAttributeValue("check", cty.StringVal(*c.Check))
		}
		if c.Assume != nil {
			cb.SetAttributeValue("assume", cty.StringVal(*c.Assume))
		}
	}

	if t.Engine != nil && t.Engine.Decoded != nil {
		writeEngine(body, t.Engine.Decoded)
	}
}

// writeColumn emits a column block with its full ColumnSpec: the type, an
// optional nullable flag, the mutually-exclusive default/materialized/ephemeral/
// alias expression, plus codec, per-column TTL, and comment. RenamedFrom is
// diff-transient metadata and intentionally not emitted. Keeping this in one
// place ensures table, materialized-view, and TimeSeries-inner columns all
// round-trip identically (issue #45).
func writeColumn(parent *hclwrite.Body, c ColumnSpec) {
	cb := parent.AppendNewBlock("column", []string{c.Name}).Body()
	cb.SetAttributeValue("type", cty.StringVal(c.Type))
	if c.Nullable {
		cb.SetAttributeValue("nullable", cty.True)
	}
	switch {
	case c.Default != nil:
		cb.SetAttributeValue("default", cty.StringVal(*c.Default))
	case c.Materialized != nil:
		cb.SetAttributeValue("materialized", cty.StringVal(*c.Materialized))
	case c.Ephemeral != nil:
		cb.SetAttributeValue("ephemeral", cty.StringVal(*c.Ephemeral))
	case c.Alias != nil:
		cb.SetAttributeValue("alias", cty.StringVal(*c.Alias))
	}
	if c.Codec != nil {
		cb.SetAttributeValue("codec", cty.StringVal(*c.Codec))
	}
	if c.TTL != nil {
		cb.SetAttributeValue("ttl", cty.StringVal(*c.TTL))
	}
	if c.Comment != nil {
		cb.SetAttributeValue("comment", cty.StringVal(*c.Comment))
	}
}

func writeEngine(parent *hclwrite.Body, e Engine) {
	block := parent.AppendNewBlock("engine", []string{e.Kind()})
	b := block.Body()
	switch v := e.(type) {
	case EngineMergeTree, EngineAggregatingMergeTree, EngineLog,
		EngineNull, EngineMemory:
		// no fields
	case EngineJoin:
		b.SetAttributeValue("strictness", cty.StringVal(v.Strictness))
		b.SetAttributeValue("type", cty.StringVal(v.JoinType))
		b.SetAttributeValue("keys", stringList(v.Keys))
	case EngineMerge:
		b.SetAttributeValue("db_regex", cty.StringVal(v.DBRegex))
		b.SetAttributeValue("table_regex", cty.StringVal(v.TableRegex))
	case EngineBuffer:
		b.SetAttributeValue("database", cty.StringVal(v.Database))
		b.SetAttributeValue("table", cty.StringVal(v.Table))
		b.SetAttributeValue("num_layers", cty.NumberIntVal(v.NumLayers))
		b.SetAttributeValue("min_time", cty.NumberIntVal(v.MinTime))
		b.SetAttributeValue("max_time", cty.NumberIntVal(v.MaxTime))
		b.SetAttributeValue("min_rows", cty.NumberIntVal(v.MinRows))
		b.SetAttributeValue("max_rows", cty.NumberIntVal(v.MaxRows))
		b.SetAttributeValue("min_bytes", cty.NumberIntVal(v.MinBytes))
		b.SetAttributeValue("max_bytes", cty.NumberIntVal(v.MaxBytes))
		if v.FlushTime != nil {
			b.SetAttributeValue("flush_time", cty.NumberIntVal(*v.FlushTime))
		}
		if v.FlushRows != nil {
			b.SetAttributeValue("flush_rows", cty.NumberIntVal(*v.FlushRows))
		}
		if v.FlushBytes != nil {
			b.SetAttributeValue("flush_bytes", cty.NumberIntVal(*v.FlushBytes))
		}
	case EngineReplicatedMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
	case EngineReplacingMergeTree:
		if v.VersionColumn != nil {
			b.SetAttributeValue("version_column", cty.StringVal(*v.VersionColumn))
		}
		if v.IsDeletedColumn != nil {
			b.SetAttributeValue("is_deleted_column", cty.StringVal(*v.IsDeletedColumn))
		}
	case EngineReplicatedReplacingMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
		if v.VersionColumn != nil {
			b.SetAttributeValue("version_column", cty.StringVal(*v.VersionColumn))
		}
		if v.IsDeletedColumn != nil {
			b.SetAttributeValue("is_deleted_column", cty.StringVal(*v.IsDeletedColumn))
		}
	case EngineSummingMergeTree:
		if len(v.SumColumns) > 0 {
			b.SetAttributeValue("sum_columns", stringList(v.SumColumns))
		}
	case EngineReplicatedSummingMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
		if len(v.SumColumns) > 0 {
			b.SetAttributeValue("sum_columns", stringList(v.SumColumns))
		}
	case EngineCollapsingMergeTree:
		b.SetAttributeValue("sign_column", cty.StringVal(v.SignColumn))
	case EngineReplicatedCollapsingMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
		b.SetAttributeValue("sign_column", cty.StringVal(v.SignColumn))
	case EngineReplicatedAggregatingMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
	case EngineDistributed:
		b.SetAttributeValue("cluster_name", cty.StringVal(v.ClusterName))
		b.SetAttributeValue("remote_database", cty.StringVal(v.RemoteDatabase))
		b.SetAttributeValue("remote_table", cty.StringVal(v.RemoteTable))
		if v.ShardingKey != nil {
			b.SetAttributeValue("sharding_key", cty.StringVal(*v.ShardingKey))
		}
		if v.PolicyName != nil {
			b.SetAttributeValue("policy_name", cty.StringVal(*v.PolicyName))
		}
	case EngineKafka:
		if v.Collection != nil {
			b.SetAttributeValue("collection", cty.StringVal(*v.Collection))
			return
		}
		setStr := func(name string, p *string) {
			if p != nil {
				b.SetAttributeValue(name, cty.StringVal(*p))
			}
		}
		setInt := func(name string, p *int64) {
			if p != nil {
				b.SetAttributeValue(name, cty.NumberIntVal(*p))
			}
		}
		setBool := func(name string, p *bool) {
			if p != nil {
				if *p {
					b.SetAttributeValue(name, cty.True)
				} else {
					b.SetAttributeValue(name, cty.False)
				}
			}
		}
		setStr("broker_list", v.BrokerList)
		setStr("topic_list", v.TopicList)
		setStr("group_name", v.GroupName)
		setStr("format", v.Format)
		setStr("security_protocol", v.SecurityProtocol)
		setStr("sasl_mechanism", v.SaslMechanism)
		setStr("sasl_username", v.SaslUsername)
		setStr("sasl_password", v.SaslPassword)
		setStr("client_id", v.ClientID)
		setStr("schema", v.Schema)
		setStr("handle_error_mode", v.HandleErrorMode)
		setStr("compression_codec", v.CompressionCodec)
		setInt("num_consumers", v.NumConsumers)
		setInt("max_block_size", v.MaxBlockSize)
		setInt("skip_broken_messages", v.SkipBrokenMessages)
		setInt("poll_timeout_ms", v.PollTimeoutMs)
		setInt("poll_max_batch_size", v.PollMaxBatchSize)
		setInt("flush_interval_ms", v.FlushIntervalMs)
		setInt("consumer_reschedule_ms", v.ConsumerRescheduleMs)
		setInt("max_rows_per_message", v.MaxRowsPerMessage)
		setInt("compression_level", v.CompressionLevel)
		setBool("commit_every_batch", v.CommitEveryBatch)
		setBool("thread_per_consumer", v.ThreadPerConsumer)
		setBool("commit_on_select", v.CommitOnSelect)
		setBool("autodetect_client_rack", v.AutodetectClientRack)
		if len(v.Extra) > 0 {
			b.SetAttributeValue("extra", stringMap(v.Extra))
		}
	case EngineTimeSeries:
		if len(v.Settings) > 0 {
			b.SetAttributeValue("settings", stringMap(v.Settings))
		}
		if len(v.TagsToColumns) > 0 {
			b.SetAttributeValue("tags_to_columns", stringMap(v.TagsToColumns))
		}
		for _, sub := range []struct {
			label string
			t     *TimeSeriesTarget
		}{
			{"samples", v.Samples},
			{"tags", v.Tags},
			{"metrics", v.Metrics},
		} {
			if sub.t == nil {
				continue
			}
			tBlock := b.AppendNewBlock(sub.label, nil)
			tb := tBlock.Body()
			if sub.t.Target != nil {
				tb.SetAttributeValue("target", cty.StringVal(*sub.t.Target))
				continue
			}
			if sub.t.Inner == nil {
				continue
			}
			innerBlock := tb.AppendNewBlock("inner", nil)
			ib := innerBlock.Body()
			for _, c := range sub.t.Inner.Columns {
				writeColumn(ib, c)
			}
			if sub.t.Inner.Engine != nil && sub.t.Inner.Engine.Decoded != nil {
				writeEngine(ib, sub.t.Inner.Engine.Decoded)
			}
			if len(sub.t.Inner.PrimaryKey) > 0 {
				ib.SetAttributeValue("primary_key", stringList(sub.t.Inner.PrimaryKey))
			}
			if len(sub.t.Inner.OrderBy) > 0 {
				ib.SetAttributeValue("order_by", stringList(sub.t.Inner.OrderBy))
			}
			if sub.t.Inner.PartitionBy != nil {
				ib.SetAttributeValue("partition_by", cty.StringVal(*sub.t.Inner.PartitionBy))
			}
			if len(sub.t.Inner.Settings) > 0 {
				ib.SetAttributeValue("settings", stringMap(sub.t.Inner.Settings))
			}
		}
	}
}

func stringList(items []string) cty.Value {
	if len(items) == 0 {
		return cty.ListValEmpty(cty.String)
	}
	vals := make([]cty.Value, len(items))
	for i, s := range items {
		vals[i] = cty.StringVal(s)
	}
	return cty.ListVal(vals)
}

func stringMap(m map[string]string) cty.Value {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	obj := make(map[string]cty.Value, len(keys))
	for _, k := range keys {
		obj[k] = cty.StringVal(m[k])
	}
	return cty.ObjectVal(obj)
}
