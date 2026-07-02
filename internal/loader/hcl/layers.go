package hcl

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// LoadLayers parses every .hcl file under each layer directory in the given
// order and merges them into a combined raw spec set. Within a layer, files
// are processed in lexical filename order. Across layers (and across files),
// a duplicate table name is an error unless the later declaration sets
// override = true. patch_table blocks always accumulate.
//
// LoadLayers does NOT call Resolve; callers run that explicitly so they can
// inspect the merged-but-unresolved input first.
func LoadLayers(layerDirs []string) (*Schema, error) {
	registry := map[string]*DatabaseSpec{}
	var ordered []string
	ncByName := map[string]*NamedCollectionSpec{}
	var ncOrder []string
	nodeByName := map[string]*NodeSpec{}
	var nodeOrder []string

	for _, dir := range layerDirs {
		files, err := hclFilesIn(dir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			parsed, err := ParseFile(file)
			if err != nil {
				return nil, err
			}
			for _, db := range parsed.Databases {
				if existing, ok := registry[db.Name]; ok {
					if err := mergeIntoDatabase(existing, db); err != nil {
						return nil, fmt.Errorf("%s: %w", file, err)
					}
				} else {
					cp := db
					registry[db.Name] = &cp
					ordered = append(ordered, db.Name)
				}
			}
			for _, nc := range parsed.NamedCollections {
				if existing, ok := ncByName[nc.Name]; ok {
					if !nc.Override {
						return nil, fmt.Errorf("%s: named_collection %q redeclared without override = true", file, nc.Name)
					}
					*existing = nc
				} else {
					cp := nc
					ncByName[nc.Name] = &cp
					ncOrder = append(ncOrder, nc.Name)
				}
			}
			for _, n := range parsed.Nodes {
				if existing, ok := nodeByName[n.Name]; ok {
					*existing = n // last declaration wins
				} else {
					cp := n
					nodeByName[n.Name] = &cp
					nodeOrder = append(nodeOrder, n.Name)
				}
			}
		}
	}

	out := &Schema{}
	for _, name := range ordered {
		out.Databases = append(out.Databases, *registry[name])
	}
	for _, name := range ncOrder {
		out.NamedCollections = append(out.NamedCollections, *ncByName[name])
	}
	for _, name := range nodeOrder {
		out.Nodes = append(out.Nodes, *nodeByName[name])
	}
	return out, nil
}

func hclFilesIn(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read layer %q: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".hcl" {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Strings(files)
	return files, nil
}

func mergeIntoDatabase(target *DatabaseSpec, incoming DatabaseSpec) error {
	indexByName := make(map[string]int, len(target.Tables))
	for i := range target.Tables {
		indexByName[target.Tables[i].Name] = i
	}
	for _, t := range incoming.Tables {
		if idx, ok := indexByName[t.Name]; ok {
			if !t.Override {
				return fmt.Errorf("table %q redeclared without override = true", t.Name)
			}
			target.Tables[idx] = t
		} else {
			target.Tables = append(target.Tables, t)
			indexByName[t.Name] = len(target.Tables) - 1
		}
	}
	target.Patches = append(target.Patches, incoming.Patches...)

	mvByName := make(map[string]bool, len(target.MaterializedViews))
	for _, mv := range target.MaterializedViews {
		mvByName[mv.Name] = true
	}
	for _, mv := range incoming.MaterializedViews {
		if mvByName[mv.Name] {
			return fmt.Errorf("materialized_view %q redeclared across layers", mv.Name)
		}
		mvByName[mv.Name] = true
		target.MaterializedViews = append(target.MaterializedViews, mv)
	}

	viewByName := make(map[string]bool, len(target.Views))
	for _, v := range target.Views {
		viewByName[v.Name] = true
	}
	for _, v := range incoming.Views {
		if viewByName[v.Name] {
			return fmt.Errorf("view %q redeclared across layers", v.Name)
		}
		viewByName[v.Name] = true
		target.Views = append(target.Views, v)
	}

	dictByName := make(map[string]bool, len(target.Dictionaries))
	for _, d := range target.Dictionaries {
		dictByName[d.Name] = true
	}
	for _, d := range incoming.Dictionaries {
		if dictByName[d.Name] {
			return fmt.Errorf("dictionary %q redeclared across layers", d.Name)
		}
		dictByName[d.Name] = true
		target.Dictionaries = append(target.Dictionaries, d)
	}

	// A raw object's identity is (kind, name), consistent with indexRaws in
	// diff.go — two kinds may legitimately share a name.
	rawKey := func(r RawSpec) string { return r.Kind + "\x00" + r.Name }
	rawSeen := make(map[string]bool, len(target.Raws))
	for _, r := range target.Raws {
		rawSeen[rawKey(r)] = true
	}
	for _, r := range incoming.Raws {
		if rawSeen[rawKey(r)] {
			return fmt.Errorf("raw %q (%s) redeclared across layers", r.Name, r.Kind)
		}
		rawSeen[rawKey(r)] = true
		target.Raws = append(target.Raws, r)
	}
	return nil
}
