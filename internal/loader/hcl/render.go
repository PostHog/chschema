package hcl

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/hclwrite"
)

// Object kinds accepted by RenderObjectSQL and RenderObjectHCL. They match the
// segment used in the web UI's object routes.
const (
	KindTable            = "table"
	KindMaterializedView = "materialized_view"
	KindView             = "view"
	KindDictionary       = "dictionary"
	KindRaw              = "raw"
)

// RenderObjectSQL returns the ClickHouse CREATE DDL for a single object named
// name of the given kind within db. It reuses the same generators that
// GenerateSQL drives, so the output matches what a fresh `diff -sql` would emit.
// It returns an error if the kind is unknown or no such object exists.
func RenderObjectSQL(database, kind, name string, db *DatabaseSpec) (string, error) {
	switch kind {
	case KindTable:
		if t := findTable(db, name); t != nil {
			return createTableSQL(database, *t), nil
		}
	case KindMaterializedView:
		if mv := findMaterializedView(db, name); mv != nil {
			return createMaterializedViewSQL(database, *mv), nil
		}
	case KindView:
		if v := findView(db, name); v != nil {
			return createViewSQL(database, *v), nil
		}
	case KindDictionary:
		if d := findDictionary(db, name); d != nil {
			return createDictionarySQL(database, *d), nil
		}
	case KindRaw:
		if r := findRaw(db, name); r != nil {
			return createRawSQL(*r), nil
		}
	default:
		return "", fmt.Errorf("render: unknown object kind %q", kind)
	}
	return "", notFound(kind, name)
}

// RenderObjectHCL returns the canonical HCL for a single object named name of
// the given kind within db, using the same writers as the schema dumper. It
// returns an error if the kind is unknown or no such object exists.
func RenderObjectHCL(database, kind, name string, db *DatabaseSpec) (string, error) {
	f := hclwrite.NewEmptyFile()
	body := f.Body()
	switch kind {
	case KindTable:
		t := findTable(db, name)
		if t == nil {
			return "", notFound(kind, name)
		}
		writeTable(body.AppendNewBlock("table", []string{t.Name}).Body(), *t)
	case KindMaterializedView:
		mv := findMaterializedView(db, name)
		if mv == nil {
			return "", notFound(kind, name)
		}
		writeMaterializedView(body.AppendNewBlock("materialized_view", []string{mv.Name}).Body(), *mv)
	case KindView:
		v := findView(db, name)
		if v == nil {
			return "", notFound(kind, name)
		}
		writeView(body.AppendNewBlock("view", []string{v.Name}).Body(), *v)
	case KindDictionary:
		d := findDictionary(db, name)
		if d == nil {
			return "", notFound(kind, name)
		}
		writeDictionary(body.AppendNewBlock("dictionary", []string{d.Name}).Body(), *d)
	case KindRaw:
		r := findRaw(db, name)
		if r == nil {
			return "", notFound(kind, name)
		}
		writeRaw(body.AppendNewBlock("raw", []string{r.Kind, r.Name}).Body(), *r)
	default:
		return "", fmt.Errorf("render: unknown object kind %q", kind)
	}
	return string(f.Bytes()), nil
}

func notFound(kind, name string) error {
	return fmt.Errorf("render: no %s named %q", kind, name)
}

// findRaw returns the raw object named name in db, or nil. The other find*
// helpers live in sql_edit.go; raw blocks aren't edited there so it's added here.
func findRaw(db *DatabaseSpec, name string) *RawSpec {
	for i := range db.Raws {
		if db.Raws[i].Name == name {
			return &db.Raws[i]
		}
	}
	return nil
}
