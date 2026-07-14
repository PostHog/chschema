package hcl

import (
	"fmt"

	"github.com/hashicorp/hcl/v2/gohcl"
)

// SourceClickHouse — SOURCE(CLICKHOUSE(...)).
type SourceClickHouse struct {
	Host            *string `hcl:"host,optional"`
	Port            *int64  `hcl:"port,optional"`
	User            *string `hcl:"user,optional"`
	Password        *string `hcl:"password,optional"`
	DB              *string `hcl:"db,optional"`
	Table           *string `hcl:"table,optional"`
	Query           *string `hcl:"query,optional"`
	InvalidateQuery *string `hcl:"invalidate_query,optional"`
	UpdateField     *string `hcl:"update_field,optional"`
	UpdateLag       *int64  `hcl:"update_lag,optional"`
}

func (SourceClickHouse) Kind() string { return "clickhouse" }

// SourceMySQL — SOURCE(MYSQL(...)).
type SourceMySQL struct {
	Host            *string `hcl:"host,optional"`
	Port            *int64  `hcl:"port,optional"`
	User            *string `hcl:"user,optional"`
	Password        *string `hcl:"password,optional"`
	DB              *string `hcl:"db,optional"`
	Table           *string `hcl:"table,optional"`
	Query           *string `hcl:"query,optional"`
	InvalidateQuery *string `hcl:"invalidate_query,optional"`
	UpdateField     *string `hcl:"update_field,optional"`
	UpdateLag       *int64  `hcl:"update_lag,optional"`
}

func (SourceMySQL) Kind() string { return "mysql" }

// SourcePostgreSQL — SOURCE(POSTGRESQL(...)).
type SourcePostgreSQL struct {
	Host            *string `hcl:"host,optional"`
	Port            *int64  `hcl:"port,optional"`
	User            *string `hcl:"user,optional"`
	Password        *string `hcl:"password,optional"`
	DB              *string `hcl:"db,optional"`
	Table           *string `hcl:"table,optional"`
	Query           *string `hcl:"query,optional"`
	InvalidateQuery *string `hcl:"invalidate_query,optional"`
	UpdateField     *string `hcl:"update_field,optional"`
	UpdateLag       *int64  `hcl:"update_lag,optional"`
}

func (SourcePostgreSQL) Kind() string { return "postgresql" }

// SourceHTTP — SOURCE(HTTP(...)).
type SourceHTTP struct {
	URL                 string  `hcl:"url"`
	Format              string  `hcl:"format"`
	CredentialsUser     *string `hcl:"credentials_user,optional"`
	CredentialsPassword *string `hcl:"credentials_password,optional"`
}

func (SourceHTTP) Kind() string { return "http" }

// SourceFile — SOURCE(FILE(...)).
type SourceFile struct {
	Path   string `hcl:"path"`
	Format string `hcl:"format"`
}

func (SourceFile) Kind() string { return "file" }

// SourceExecutable — SOURCE(EXECUTABLE(...)).
type SourceExecutable struct {
	Command     string `hcl:"command"`
	Format      string `hcl:"format"`
	ImplicitKey *bool  `hcl:"implicit_key,optional"`
}

func (SourceExecutable) Kind() string { return "executable" }

// SourceNull — SOURCE(NULL()).
type SourceNull struct{}

func (SourceNull) Kind() string { return "null" }

// dictSecret returns the source's credential field: its DDL argument name and
// its value (nil when unset). At most one field per source kind is a secret —
// file/executable/null have none, so they report ("", nil).
//
// A secret's value may be the RedactedValue marker, meaning hclexp does not
// know the real one: the server redacted it at introspection, or the author
// declared it unmanaged. withDictSecret is the setter half; together they let
// the diff mask an unknown secret out of a comparison without every source
// kind having to know about redaction.
func dictSecret(s DictionarySource) (field string, value *string) {
	switch v := s.(type) {
	case SourceClickHouse:
		return "password", v.Password
	case SourceMySQL:
		return "password", v.Password
	case SourcePostgreSQL:
		return "password", v.Password
	case SourceHTTP:
		return "credentials_password", v.CredentialsPassword
	}
	return "", nil
}

// withDictSecret returns a copy of s with its credential field set to value.
// Sources are small value structs, so the copy is an assignment. A source kind
// with no secret is returned unchanged.
func withDictSecret(s DictionarySource, value *string) DictionarySource {
	switch v := s.(type) {
	case SourceClickHouse:
		v.Password = value
		return v
	case SourceMySQL:
		v.Password = value
		return v
	case SourcePostgreSQL:
		v.Password = value
		return v
	case SourceHTTP:
		v.CredentialsPassword = value
		return v
	}
	return s
}

// dictSecretIsRedacted reports whether the source's credential is the
// RedactedValue marker, i.e. hclexp does not know the real secret.
func dictSecretIsRedacted(s DictionarySource) (field string, redacted bool) {
	field, v := dictSecret(s)
	return field, v != nil && *v == RedactedValue
}

// DecodeDictionarySource dispatches on spec.Kind and decodes the body into
// the matching typed source struct. Returns (nil, nil) when spec is nil.
func DecodeDictionarySource(spec *DictionarySourceSpec) (DictionarySource, error) {
	if spec == nil {
		return nil, nil
	}
	switch spec.Kind {
	case "clickhouse":
		var s SourceClickHouse
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source clickhouse: %s", d.Error())
		}
		return s, nil
	case "mysql":
		var s SourceMySQL
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source mysql: %s", d.Error())
		}
		return s, nil
	case "postgresql":
		var s SourcePostgreSQL
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source postgresql: %s", d.Error())
		}
		return s, nil
	case "http":
		var s SourceHTTP
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source http: %s", d.Error())
		}
		return s, nil
	case "file":
		var s SourceFile
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source file: %s", d.Error())
		}
		return s, nil
	case "executable":
		var s SourceExecutable
		if d := gohcl.DecodeBody(spec.Body, nil, &s); d.HasErrors() {
			return nil, fmt.Errorf("source executable: %s", d.Error())
		}
		return s, nil
	case "null":
		return SourceNull{}, nil
	default:
		return nil, fmt.Errorf("unsupported dictionary source kind %q", spec.Kind)
	}
}
