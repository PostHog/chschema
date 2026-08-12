package hcl

// objectIdentity is a logical ClickHouse schema-object identity. Database
// objects share one namespace regardless of whether hclexp currently models
// them structurally or as raw DDL; named collections use the top-level
// namespace.
type objectIdentity struct {
	database string
	name     string
	named    bool
}

// ScopeSchemaToObjects returns a non-mutating copy of source containing only
// objects whose logical identity occurs in scope. Database wrappers and node
// metadata are retained. It is the shared primitive behind directional diff
// scope and desired-scoped topology planning.
func ScopeSchemaToObjects(source, scope *Schema) *Schema {
	if source == nil {
		return &Schema{}
	}

	owned := schemaObjectIdentities(scope)
	out := &Schema{
		Databases: make([]DatabaseSpec, len(source.Databases)),
		Nodes:     append([]NodeSpec(nil), source.Nodes...),
	}
	for i, db := range source.Databases {
		copyDB := db
		copyDB.Tables = keepScoped(db.Tables, func(v TableSpec) objectIdentity {
			return databaseObjectIdentity(db.Name, v.Name)
		}, owned)
		copyDB.MaterializedViews = keepScoped(db.MaterializedViews, func(v MaterializedViewSpec) objectIdentity {
			return databaseObjectIdentity(db.Name, v.Name)
		}, owned)
		copyDB.Views = keepScoped(db.Views, func(v ViewSpec) objectIdentity {
			return databaseObjectIdentity(db.Name, v.Name)
		}, owned)
		copyDB.Dictionaries = keepScoped(db.Dictionaries, func(v DictionarySpec) objectIdentity {
			return databaseObjectIdentity(db.Name, v.Name)
		}, owned)
		copyDB.Raws = keepScoped(db.Raws, func(v RawSpec) objectIdentity {
			return databaseObjectIdentity(db.Name, v.Name)
		}, owned)
		out.Databases[i] = copyDB
	}
	out.NamedCollections = keepScoped(source.NamedCollections, func(v NamedCollectionSpec) objectIdentity {
		return namedCollectionIdentity(v.Name)
	}, owned)
	return out
}

func schemaObjectIdentities(schema *Schema) map[objectIdentity]struct{} {
	out := map[objectIdentity]struct{}{}
	if schema == nil {
		return out
	}
	for _, db := range schema.Databases {
		add := func(name string) { out[databaseObjectIdentity(db.Name, name)] = struct{}{} }
		for _, v := range db.Tables {
			add(v.Name)
		}
		for _, v := range db.MaterializedViews {
			add(v.Name)
		}
		for _, v := range db.Views {
			add(v.Name)
		}
		for _, v := range db.Dictionaries {
			add(v.Name)
		}
		for _, v := range db.Raws {
			add(v.Name)
		}
	}
	for _, v := range schema.NamedCollections {
		out[namedCollectionIdentity(v.Name)] = struct{}{}
	}
	return out
}

func databaseObjectIdentity(database, name string) objectIdentity {
	return objectIdentity{database: database, name: name}
}

func namedCollectionIdentity(name string) objectIdentity {
	return objectIdentity{name: name, named: true}
}

func keepScoped[T any](source []T, identity func(T) objectIdentity, owned map[objectIdentity]struct{}) []T {
	out := make([]T, 0, len(source))
	for _, v := range source {
		if _, ok := owned[identity(v)]; ok {
			out = append(out, v)
		}
	}
	return out
}
