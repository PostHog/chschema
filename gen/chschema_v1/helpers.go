package chschema_v1

// FindTableByName searches for a table by name in a slice, returns nil if not found
func FindTableByName(tables []*Table, name string) *Table {
	for _, v := range tables {
		if v.Name == name {
			return v
		}
	}
	return nil
}

// FindColumnByName searches for a column by name in a slice, returns nil if not found
func FindColumnByName(columns []*Column, name string) *Column {
	for _, v := range columns {
		if v.Name == name {
			return v
		}
	}
	return nil
}

// FindViewByName searches for a view by name in a slice, returns nil if not found
func FindViewByName(views []*View, name string) *View {
	for _, v := range views {
		if v.Name == name {
			return v
		}
	}
	return nil
}

// FindMaterializedViewByName searches for a materialized view by name in a slice, returns nil if not found
func FindMaterializedViewByName(views []*MaterializedView, name string) *MaterializedView {
	for _, v := range views {
		if v.Name == name {
			return v
		}
	}
	return nil
}

// FindClusterByName searches for a cluster by name in a slice, returns nil if not found
func FindClusterByName(clusters []*Cluster, name string) *Cluster {
	for _, v := range clusters {
		if v.Name == name {
			return v
		}
	}
	return nil
}
