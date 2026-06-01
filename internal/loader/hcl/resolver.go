package hcl

import (
	"errors"
	"fmt"
	"strings"
)

// Resolve walks each database, applies patch_table additions, resolves
// extend chains, drops abstract tables, and validates that every remaining
// table has an engine. All mutation happens in place on the supplied slice.
func Resolve(s *Schema) error {
	if s == nil {
		return errors.New("Resolve: nil schema")
	}
	if err := validateNamedCollections(s); err != nil {
		return err
	}
	for di := range s.Databases {
		if err := applyPatches(&s.Databases[di]); err != nil {
			return err
		}
		if err := resolveDatabase(&s.Databases[di]); err != nil {
			return err
		}
		if err := validateDictionaries(&s.Databases[di]); err != nil {
			return err
		}
		if err := validateViews(&s.Databases[di]); err != nil {
			return err
		}
	}
	if err := validateKafkaEngines(s); err != nil {
		return err
	}
	return nil
}

// validateNamedCollections enforces name uniqueness, param-key uniqueness,
// and that Params is non-empty for managed (non-external) collections.
func validateNamedCollections(s *Schema) error {
	seen := map[string]bool{}
	for _, nc := range s.NamedCollections {
		if seen[nc.Name] {
			return fmt.Errorf("named_collection %q: duplicate", nc.Name)
		}
		seen[nc.Name] = true
		if !nc.External && len(nc.Params) == 0 {
			return fmt.Errorf("named_collection %q: requires non-empty params (or external = true)", nc.Name)
		}
		keys := map[string]bool{}
		for _, p := range nc.Params {
			if keys[p.Key] {
				return fmt.Errorf("named_collection %q: duplicate param %q", nc.Name, p.Key)
			}
			keys[p.Key] = true
		}
	}
	return nil
}

// validateKafkaEngines enforces XOR between collection and inline settings,
// required-fields-when-inline, and that referenced collections exist.
func validateKafkaEngines(s *Schema) error {
	ncDeclared := map[string]bool{}
	for _, nc := range s.NamedCollections {
		ncDeclared[nc.Name] = true
	}
	for _, db := range s.Databases {
		for _, t := range db.Tables {
			if t.Engine == nil || t.Engine.Decoded == nil {
				continue
			}
			k, ok := t.Engine.Decoded.(EngineKafka)
			if !ok {
				continue
			}
			hasInline := k.BrokerList != nil || k.TopicList != nil || k.GroupName != nil || k.Format != nil ||
				k.SecurityProtocol != nil || k.SaslMechanism != nil || k.SaslUsername != nil || k.SaslPassword != nil ||
				k.ClientID != nil || k.Schema != nil || k.HandleErrorMode != nil || k.CompressionCodec != nil ||
				k.NumConsumers != nil || k.MaxBlockSize != nil || k.SkipBrokenMessages != nil ||
				k.PollTimeoutMs != nil || k.PollMaxBatchSize != nil || k.FlushIntervalMs != nil ||
				k.ConsumerRescheduleMs != nil || k.MaxRowsPerMessage != nil || k.CompressionLevel != nil ||
				k.CommitEveryBatch != nil || k.ThreadPerConsumer != nil || k.CommitOnSelect != nil ||
				k.AutodetectClientRack != nil || len(k.Extra) > 0

			if k.Collection == nil && !hasInline {
				return fmt.Errorf("%s.%s: kafka engine requires either `collection` or inline settings", db.Name, t.Name)
			}
			if k.Collection != nil && hasInline {
				return fmt.Errorf("%s.%s: kafka engine `collection` and inline settings are mutually exclusive", db.Name, t.Name)
			}
			if k.Collection != nil {
				if !ncDeclared[*k.Collection] {
					return fmt.Errorf("%s.%s: kafka engine references collection %q which is not declared in the schema (declare with `named_collection %q {...}` or `external = true`)", db.Name, t.Name, *k.Collection, *k.Collection)
				}
			} else {
				if k.BrokerList == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires broker_list", db.Name, t.Name)
				}
				if k.TopicList == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires topic_list", db.Name, t.Name)
				}
				if k.GroupName == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires group_name", db.Name, t.Name)
				}
				if k.Format == nil {
					return fmt.Errorf("%s.%s: kafka engine inline form requires format", db.Name, t.Name)
				}
			}
		}
	}
	return nil
}

// validateDictionaries enforces dictionary-specific invariants: each dict
// must have exactly one source and one layout, a non-empty primary key, and
// a range block only when the layout is one of the range_hashed variants.
func validateDictionaries(db *DatabaseSpec) error {
	for _, d := range db.Dictionaries {
		if d.Source == nil {
			return fmt.Errorf("%s.%s: dictionary requires a source block", db.Name, d.Name)
		}
		if d.Layout == nil {
			return fmt.Errorf("%s.%s: dictionary requires a layout block", db.Name, d.Name)
		}
		if len(d.PrimaryKey) == 0 {
			return fmt.Errorf("%s.%s: dictionary requires a non-empty primary_key", db.Name, d.Name)
		}
		if d.Range != nil {
			switch d.Layout.Kind {
			case "range_hashed", "complex_key_range_hashed":
				// allowed
			default:
				return fmt.Errorf("%s.%s: range block only allowed with range_hashed or complex_key_range_hashed layouts (got %q)", db.Name, d.Name, d.Layout.Kind)
			}
		}
	}
	return nil
}

// validateViews normalises and validates parsed ViewSpec entries. The HCL
// surface is more permissive than ClickHouse semantics; this catches
// schema mistakes at parse time rather than at apply.
func validateViews(db *DatabaseSpec) error {
	for i := range db.Views {
		v := &db.Views[i]
		if v.SQLSecurity != nil {
			canonical := strings.ToLower(strings.TrimSpace(*v.SQLSecurity))
			switch canonical {
			case "definer", "invoker", "none":
				v.SQLSecurity = &canonical
			default:
				return fmt.Errorf(`%s.%s: sql_security must be one of "definer", "invoker", "none" (got %q)`, db.Name, v.Name, *v.SQLSecurity)
			}
		}
		if v.Definer != nil {
			if v.SQLSecurity == nil || *v.SQLSecurity != "definer" {
				return fmt.Errorf(`%s.%s: definer requires sql_security = "definer"`, db.Name, v.Name)
			}
		}
	}
	return nil
}

func applyPatches(db *DatabaseSpec) error {
	if len(db.Patches) == 0 {
		return nil
	}
	indexByName := make(map[string]int, len(db.Tables))
	for i := range db.Tables {
		indexByName[db.Tables[i].Name] = i
	}
	for _, patch := range db.Patches {
		idx, ok := indexByName[patch.Name]
		if !ok {
			return fmt.Errorf("%s: patch_table %q references unknown table", db.Name, patch.Name)
		}
		target := &db.Tables[idx]
		seen := make(map[string]bool, len(target.Columns)+len(patch.Columns))
		for _, c := range target.Columns {
			seen[c.Name] = true
		}
		for _, c := range patch.Columns {
			if seen[c.Name] {
				return fmt.Errorf("%s: patch_table %q: column %q already exists on target", db.Name, patch.Name, c.Name)
			}
			seen[c.Name] = true
			target.Columns = append(target.Columns, c)
		}
	}
	db.Patches = nil
	return nil
}

func resolveDatabase(db *DatabaseSpec) error {
	indexByName := make(map[string]int, len(db.Tables))
	for i := range db.Tables {
		name := db.Tables[i].Name
		if _, dup := indexByName[name]; dup {
			return fmt.Errorf("%s: duplicate table %q", db.Name, name)
		}
		indexByName[name] = i
	}

	resolved := make(map[string]bool, len(db.Tables))
	visiting := make(map[string]bool, len(db.Tables))

	for i := range db.Tables {
		if err := resolveTable(db, i, indexByName, resolved, visiting); err != nil {
			return err
		}
	}

	// MV resolution must happen BEFORE the abstract-table drop below, so
	// abstract parents are still present in db.Tables / indexByName when
	// each extending MV looks them up.
	mvIndex := make(map[string]bool, len(db.MaterializedViews))
	for i := range db.MaterializedViews {
		name := db.MaterializedViews[i].Name
		if mvIndex[name] {
			return fmt.Errorf("%s: duplicate materialized_view %q", db.Name, name)
		}
		mvIndex[name] = true
	}
	for i := range db.MaterializedViews {
		if err := resolveMaterializedView(db, i, indexByName, mvIndex); err != nil {
			return err
		}
	}

	kept := db.Tables[:0]
	for _, t := range db.Tables {
		if !t.Abstract {
			kept = append(kept, t)
		}
	}
	db.Tables = kept

	keptMVs := db.MaterializedViews[:0]
	for _, mv := range db.MaterializedViews {
		if !mv.Abstract {
			keptMVs = append(keptMVs, mv)
		}
	}
	db.MaterializedViews = keptMVs

	// Cascade the database-level cluster default into each table and MV
	// that hasn't set its own. Done after abstracts are dropped so we
	// never touch objects that won't be emitted.
	if db.Cluster != nil {
		for i := range db.Tables {
			if db.Tables[i].Cluster == nil {
				v := *db.Cluster
				db.Tables[i].Cluster = &v
			}
		}
		for i := range db.MaterializedViews {
			if db.MaterializedViews[i].Cluster == nil {
				v := *db.Cluster
				db.MaterializedViews[i].Cluster = &v
			}
		}
	}

	for _, t := range db.Tables {
		if t.Engine == nil || t.Engine.Decoded == nil {
			return fmt.Errorf("%s.%s: non-abstract table requires an engine", db.Name, t.Name)
		}
		if err := validateColumns(db.Name, t); err != nil {
			return err
		}
		if err := validateConstraints(db.Name, t); err != nil {
			return err
		}
	}
	for _, mv := range db.MaterializedViews {
		if mv.ToTable == "" {
			return fmt.Errorf("%s.%s: materialized_view requires to_table", db.Name, mv.Name)
		}
		if mv.Query == "" {
			return fmt.Errorf("%s.%s: materialized_view requires query", db.Name, mv.Name)
		}
	}
	return nil
}

func validateConstraints(db string, t TableSpec) error {
	for _, c := range t.Constraints {
		set := 0
		if c.Check != nil {
			set++
		}
		if c.Assume != nil {
			set++
		}
		if set != 1 {
			return fmt.Errorf("%s.%s.constraint[%s]: exactly one of check, assume must be set", db, t.Name, c.Name)
		}
	}
	return nil
}

// validateColumns checks mutually-exclusive default-value attributes and
// rejects the Nullable + nullable=true combination per ClickHouse's rule.
func validateColumns(db string, t TableSpec) error {
	for _, c := range t.Columns {
		set := 0
		if c.Default != nil {
			set++
		}
		if c.Materialized != nil {
			set++
		}
		if c.Ephemeral != nil {
			set++
		}
		if c.Alias != nil {
			set++
		}
		if set > 1 {
			return fmt.Errorf("%s.%s.%s: at most one of default, materialized, ephemeral, alias may be set", db, t.Name, c.Name)
		}
		if c.Nullable && strings.HasPrefix(c.Type, "Nullable(") {
			return fmt.Errorf("%s.%s.%s: cannot combine nullable = true with a Nullable(...) type", db, t.Name, c.Name)
		}
	}
	return nil
}

func resolveTable(db *DatabaseSpec, idx int, indexByName map[string]int, resolved, visiting map[string]bool) error {
	t := &db.Tables[idx]
	if resolved[t.Name] {
		return nil
	}
	if visiting[t.Name] {
		return fmt.Errorf("%s.%s: cycle in extend chain", db.Name, t.Name)
	}

	if t.Extend == nil {
		resolved[t.Name] = true
		return nil
	}

	parentName := *t.Extend
	parentIdx, ok := indexByName[parentName]
	if !ok {
		return fmt.Errorf("%s.%s: extend references unknown table %q", db.Name, t.Name, parentName)
	}

	visiting[t.Name] = true
	if err := resolveTable(db, parentIdx, indexByName, resolved, visiting); err != nil {
		return err
	}
	delete(visiting, t.Name)

	parent := &db.Tables[parentIdx]
	if err := mergeParent(t, parent); err != nil {
		return fmt.Errorf("%s.%s: %w", db.Name, t.Name, err)
	}
	t.Extend = nil
	resolved[t.Name] = true
	return nil
}

// resolveMaterializedView merges an MV's `extend` parent (an abstract
// table) into the MV. Only column/cluster/comment flow from the parent;
// engine, order_by, etc. are table-only concepts. Must run before
// abstract tables are dropped from db.Tables.
func resolveMaterializedView(db *DatabaseSpec, idx int, tableIndex map[string]int, mvIndex map[string]bool) error {
	mv := &db.MaterializedViews[idx]
	if mv.Extend == nil {
		return nil
	}
	parentName := *mv.Extend
	if mvIndex[parentName] {
		return fmt.Errorf("%s.%s: materialized_view cannot extend another materialized_view %q (extend an abstract table instead)", db.Name, mv.Name, parentName)
	}
	parentIdx, ok := tableIndex[parentName]
	if !ok {
		return fmt.Errorf("%s.%s: extend references unknown table %q", db.Name, mv.Name, parentName)
	}
	parent := &db.Tables[parentIdx]
	if !parent.Abstract {
		return fmt.Errorf("%s.%s: extend target %q must be an abstract table", db.Name, mv.Name, parentName)
	}

	if len(parent.Columns) > 0 || len(mv.Columns) > 0 {
		merged := make([]ColumnSpec, 0, len(parent.Columns)+len(mv.Columns))
		seen := make(map[string]bool, len(parent.Columns)+len(mv.Columns))
		for _, c := range parent.Columns {
			seen[c.Name] = true
			merged = append(merged, c)
		}
		for _, c := range mv.Columns {
			if seen[c.Name] {
				return fmt.Errorf("%s.%s: column %q collides with inherited column", db.Name, mv.Name, c.Name)
			}
			seen[c.Name] = true
			merged = append(merged, c)
		}
		mv.Columns = merged
	}
	if mv.Cluster == nil && parent.Cluster != nil {
		v := *parent.Cluster
		mv.Cluster = &v
	}
	if mv.Comment == nil && parent.Comment != nil {
		v := *parent.Comment
		mv.Comment = &v
	}
	mv.Extend = nil
	return nil
}

func mergeParent(child, parent *TableSpec) error {
	if len(parent.Columns) > 0 || len(child.Columns) > 0 {
		merged := make([]ColumnSpec, 0, len(parent.Columns)+len(child.Columns))
		seen := make(map[string]bool, len(parent.Columns)+len(child.Columns))
		for _, c := range parent.Columns {
			seen[c.Name] = true
			merged = append(merged, c)
		}
		for _, c := range child.Columns {
			if seen[c.Name] {
				return fmt.Errorf("column %q collides with inherited column", c.Name)
			}
			seen[c.Name] = true
			merged = append(merged, c)
		}
		child.Columns = merged
	}

	if len(parent.Indexes) > 0 || len(child.Indexes) > 0 {
		merged := make([]IndexSpec, 0, len(parent.Indexes)+len(child.Indexes))
		seen := make(map[string]bool, len(parent.Indexes)+len(child.Indexes))
		for _, idx := range parent.Indexes {
			seen[idx.Name] = true
			merged = append(merged, idx)
		}
		for _, idx := range child.Indexes {
			if seen[idx.Name] {
				return fmt.Errorf("index %q collides with inherited index", idx.Name)
			}
			seen[idx.Name] = true
			merged = append(merged, idx)
		}
		child.Indexes = merged
	}

	if child.OrderBy == nil && parent.OrderBy != nil {
		child.OrderBy = append([]string(nil), parent.OrderBy...)
	}
	if child.PartitionBy == nil && parent.PartitionBy != nil {
		v := *parent.PartitionBy
		child.PartitionBy = &v
	}
	if child.SampleBy == nil && parent.SampleBy != nil {
		v := *parent.SampleBy
		child.SampleBy = &v
	}
	if child.TTL == nil && parent.TTL != nil {
		v := *parent.TTL
		child.TTL = &v
	}
	if child.Settings == nil && parent.Settings != nil {
		s := make(map[string]string, len(parent.Settings))
		for k, v := range parent.Settings {
			s[k] = v
		}
		child.Settings = s
	}
	if child.Engine == nil && parent.Engine != nil {
		eng := *parent.Engine
		child.Engine = &eng
	}
	return nil
}
