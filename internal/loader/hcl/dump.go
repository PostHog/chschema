package hcl

import (
	"io"
	"sort"

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
func Write(w io.Writer, dbs []DatabaseSpec) error {
	f := hclwrite.NewEmptyFile()
	body := f.Body()

	for i, db := range dbs {
		if i > 0 {
			body.AppendNewline()
		}
		dbBlock := body.AppendNewBlock("database", []string{db.Name})
		writeDatabase(dbBlock.Body(), db)
	}

	_, err := w.Write(f.Bytes())
	return err
}

func writeDatabase(body *hclwrite.Body, db DatabaseSpec) {
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

	dicts := append([]DictionarySpec(nil), db.Dictionaries...)
	sort.Slice(dicts, func(i, j int) bool { return dicts[i].Name < dicts[j].Name })
	for i, d := range dicts {
		if len(tables) > 0 || len(mvs) > 0 || i > 0 {
			body.AppendNewline()
		}
		dBlock := body.AppendNewBlock("dictionary", []string{d.Name})
		writeDictionary(dBlock.Body(), d)
	}
}

func writeMaterializedView(body *hclwrite.Body, mv MaterializedViewSpec) {
	body.SetAttributeValue("to_table", cty.StringVal(mv.ToTable))
	body.SetAttributeValue("query", cty.StringVal(mv.Query))
	if mv.Cluster != nil {
		body.SetAttributeValue("cluster", cty.StringVal(*mv.Cluster))
	}
	if mv.Comment != nil {
		body.SetAttributeValue("comment", cty.StringVal(*mv.Comment))
	}
	for _, c := range mv.Columns {
		colBlock := body.AppendNewBlock("column", []string{c.Name})
		colBlock.Body().SetAttributeValue("type", cty.StringVal(c.Type))
	}
}

func writeTable(body *hclwrite.Body, t TableSpec) {
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
		colBlock := body.AppendNewBlock("column", []string{c.Name})
		colBlock.Body().SetAttributeValue("type", cty.StringVal(c.Type))
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

	if t.Engine != nil && t.Engine.Decoded != nil {
		writeEngine(body, t.Engine.Decoded)
	}
}

func writeEngine(parent *hclwrite.Body, e Engine) {
	block := parent.AppendNewBlock("engine", []string{e.Kind()})
	b := block.Body()
	switch v := e.(type) {
	case EngineMergeTree, EngineAggregatingMergeTree, EngineLog:
		// no fields
	case EngineReplicatedMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
	case EngineReplacingMergeTree:
		if v.VersionColumn != nil {
			b.SetAttributeValue("version_column", cty.StringVal(*v.VersionColumn))
		}
	case EngineReplicatedReplacingMergeTree:
		b.SetAttributeValue("zoo_path", cty.StringVal(v.ZooPath))
		b.SetAttributeValue("replica_name", cty.StringVal(v.ReplicaName))
		if v.VersionColumn != nil {
			b.SetAttributeValue("version_column", cty.StringVal(*v.VersionColumn))
		}
	case EngineSummingMergeTree:
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
	case EngineKafka:
		b.SetAttributeValue("broker_list", stringList(v.BrokerList))
		b.SetAttributeValue("topic", cty.StringVal(v.Topic))
		b.SetAttributeValue("consumer_group", cty.StringVal(v.ConsumerGroup))
		b.SetAttributeValue("format", cty.StringVal(v.Format))
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
