package hcl

import (
	"strings"
	"testing"

	chparser "github.com/orian/clickhouse-sql-parser/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Issue #91: three parse sites silently swallowed errors, turning malformed
// input into zero values (granularity 0, absent lifetime, empty engine clause)
// that then round-trip as phantom drift. These tests lock in loud propagation.

// Site 1 — indexFromAST must surface a malformed GRANULARITY rather than
// yield granularity 0.
func TestIndexFromAST_MalformedGranularityErrors(t *testing.T) {
	_, err := indexFromAST(&chparser.TableIndex{
		Granularity: &chparser.NumberLiteral{Literal: "not_a_number"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not_a_number")
}

func TestIndexFromAST_ValidGranularity(t *testing.T) {
	got, err := indexFromAST(&chparser.TableIndex{
		Granularity: &chparser.NumberLiteral{Literal: "4"},
	})
	require.NoError(t, err)
	assert.Equal(t, 4, got.Granularity)
}

// Site 3 — parseInt64Literal must surface a malformed integer literal rather
// than yield 0; nil (absent) stays 0 without error.
func TestParseInt64Literal_MalformedErrors(t *testing.T) {
	_, err := parseInt64Literal(&chparser.NumberLiteral{Literal: "12x9"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "12x9")
}

func TestParseInt64Literal_NilAndValid(t *testing.T) {
	v, err := parseInt64Literal(nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), v)

	v, err = parseInt64Literal(&chparser.NumberLiteral{Literal: "3600"})
	require.NoError(t, err)
	assert.Equal(t, int64(3600), v)
}

// Site 2 — the TimeSeries inner-engine renderer must not silently drop engine
// settings. Inline Kafka is the only engine that yields settings and is not a
// valid inner engine, but if one appears the settings must surface rather than
// vanish.
func TestEmitTimeSeriesTarget_InnerEngineSettingsNotDropped(t *testing.T) {
	var b strings.Builder
	emitTimeSeriesTarget(&b, &TimeSeriesTarget{
		Inner: &TimeSeriesInnerTable{
			Columns: []ColumnSpec{{Name: "id", Type: "UUID"}},
			Engine: &EngineSpec{Kind: "kafka", Decoded: EngineKafka{
				BrokerList: ptr("localhost:9092"),
				TopicList:  ptr("t"),
			}},
		},
	}, "SAMPLES")

	out := b.String()
	assert.Contains(t, out, "INNER ENGINE = Kafka()")
	assert.Contains(t, out, "SETTINGS", "inner-engine settings must not be dropped")
	assert.Contains(t, out, "kafka_broker_list")
}
