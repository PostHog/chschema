package hcl

import "sort"

// InferExternalNamedCollections declares collection references that are
// missing from a captured schema as external. Live ClickHouse users can be
// allowed to use config-backed named collections without being allowed to
// enumerate them through system.named_collections. In that case introspection
// still sees the collection name in table/dictionary DDL, but cannot capture
// its values or an explicit declaration.
//
// This is intentionally not part of Resolve: authored schemas must continue to
// declare every collection explicitly. Callers use it only at the live/dump
// boundary, where a reference is evidence that the collection is provisioned
// outside the captured schema. The returned names are the declarations added,
// sorted for deterministic diagnostics and dumps.
func InferExternalNamedCollections(s *Schema) []string {
	if s == nil {
		return nil
	}

	declared := make(map[string]bool, len(s.NamedCollections))
	for _, collection := range s.NamedCollections {
		declared[collection.Name] = true
	}

	missing := map[string]bool{}
	add := func(collection *string) {
		if collection == nil || *collection == "" || declared[*collection] {
			return
		}
		missing[*collection] = true
	}
	for _, database := range s.Databases {
		for _, table := range database.Tables {
			if table.Engine == nil || table.Engine.Decoded == nil {
				continue
			}
			if kafka, ok := table.Engine.Decoded.(EngineKafka); ok {
				add(kafka.Collection)
			}
		}
		for _, dictionary := range database.Dictionaries {
			if dictionary.Source == nil || dictionary.Source.Decoded == nil {
				continue
			}
			add(dictSourceCollection(dictionary.Source.Decoded))
		}
	}

	names := make([]string, 0, len(missing))
	for name := range missing {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s.NamedCollections = append(s.NamedCollections, NamedCollectionSpec{Name: name, External: true})
	}
	return names
}
