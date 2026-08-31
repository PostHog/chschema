package hcl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDictionaryFileSource_StringLiteralEscaping(t *testing.T) {
	const wantPath = "chschema_audit_file'quote\\part.tsv"
	const wantFormat = "Tab'Separated\\Raw"

	got := sourceSQL(SourceFile{Path: wantPath, Format: wantFormat})
	assert.Equal(t, `FILE(PATH 'chschema_audit_file\'quote\\part.tsv' FORMAT 'Tab\'Separated\\Raw')`, got)

	dictionary := rtDictFromDDL(t, fmt.Sprintf(rtDictSourceDDL, got))
	fileSource, ok := dictionary.Source.Decoded.(SourceFile)
	require.True(t, ok)
	assert.Equal(t, SourceFile{Path: wantPath, Format: wantFormat}, fileSource)
}

func TestDictionaryFileSource_DecodesAcceptedApostropheSpellings(t *testing.T) {
	const wantPath = "chschema_audit_file'quote\\part.tsv"

	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "show_create_backslash_escape", path: `chschema_audit_file\'quote\\part.tsv`},
		{name: "doubled_quote_input", path: `chschema_audit_file''quote\\part.tsv`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dictionary := rtDictFromDDL(t, fmt.Sprintf(rtDictSourceDDL,
				"FILE(PATH '"+tc.path+"' FORMAT 'TabSeparated')"))
			fileSource, ok := dictionary.Source.Decoded.(SourceFile)
			require.True(t, ok)
			assert.Equal(t, wantPath, fileSource.Path)

			dumped := rtFlatWS(rtDumpDictHCL(t, dictionary))
			assert.Contains(t, dumped, `path = "chschema_audit_file'quote\\part.tsv"`)
			assert.NotContains(t, dumped, `file\\'quote`)
		})
	}
}
