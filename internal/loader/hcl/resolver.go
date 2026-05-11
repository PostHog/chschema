package hcl

import "fmt"

// Resolve walks each database, applies patch_table additions, resolves
// extend chains, drops abstract tables, and validates that every remaining
// table has an engine. All mutation happens in place on the supplied slice.
func Resolve(dbs []DatabaseSpec) error {
	for di := range dbs {
		if err := applyPatches(&dbs[di]); err != nil {
			return err
		}
		if err := resolveDatabase(&dbs[di]); err != nil {
			return err
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

	kept := db.Tables[:0]
	for _, t := range db.Tables {
		if !t.Abstract {
			kept = append(kept, t)
		}
	}
	db.Tables = kept

	for _, t := range db.Tables {
		if t.Engine == nil || t.Engine.Decoded == nil {
			return fmt.Errorf("%s.%s: non-abstract table requires an engine", db.Name, t.Name)
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
