package hcl

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDriverRows serves canned string-column rows as a driver.Rows. Only the
// methods macros introspection touches are implemented; the embedded nil
// interface makes any other call panic loudly.
type fakeDriverRows struct {
	driver.Rows
	rows    [][]string
	pos     int
	scanErr error
	iterErr error
	closed  bool
}

func (r *fakeDriverRows) Next() bool {
	r.pos++
	return r.pos <= len(r.rows)
}

func (r *fakeDriverRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	for i, d := range dest {
		*d.(*string) = r.rows[r.pos-1][i]
	}
	return nil
}

func (r *fakeDriverRows) Err() error { return r.iterErr }

func (r *fakeDriverRows) Close() error {
	r.closed = true
	return nil
}

// fakeDriverConn is a driver.Conn whose Query dispatches on the query text;
// all other methods panic via the embedded nil interface.
type fakeDriverConn struct {
	driver.Conn
	query func(query string) (driver.Rows, error)
}

func (c *fakeDriverConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	return c.query(query)
}

func connReturning(rows driver.Rows, err error) *fakeDriverConn {
	return &fakeDriverConn{query: func(string) (driver.Rows, error) { return rows, err }}
}

func TestMacrosIntrospect_HappyPath(t *testing.T) {
	rows := &fakeDriverRows{rows: [][]string{
		{"hostClusterRole", "online"},
		{"hostClusterType", "posthog"},
		{"replica", "ch-1-1"},
		{"shard", "01"},
	}}

	got, err := IntrospectMacros(context.Background(), connReturning(rows, nil))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"hostClusterRole": "online",
		"hostClusterType": "posthog",
		"replica":         "ch-1-1",
		"shard":           "01",
	}, got)
	assert.True(t, rows.closed)
}

func TestMacrosIntrospect_Empty(t *testing.T) {
	got, err := IntrospectMacros(context.Background(), connReturning(&fakeDriverRows{}, nil))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, got)
}

func TestMacrosIntrospect_QueryError(t *testing.T) {
	errBoom := errors.New("boom")

	got, err := IntrospectMacros(context.Background(), connReturning(nil, errBoom))
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "query system.macros")
	assert.Nil(t, got)
}

func TestMacrosIntrospect_ScanError(t *testing.T) {
	errBoom := errors.New("boom")
	rows := &fakeDriverRows{rows: [][]string{{"shard", "01"}}, scanErr: errBoom}

	got, err := IntrospectMacros(context.Background(), connReturning(rows, nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "scan system.macros")
	assert.Nil(t, got)
}

func TestMacrosIntrospect_IterationError(t *testing.T) {
	errBoom := errors.New("boom")
	rows := &fakeDriverRows{rows: [][]string{{"shard", "01"}}, iterErr: errBoom}

	_, err := IntrospectMacros(context.Background(), connReturning(rows, nil))
	assert.ErrorIs(t, err, errBoom)
}

func TestMacrosIntrospectNode_NameOverride(t *testing.T) {
	conn := &fakeDriverConn{query: func(q string) (driver.Rows, error) {
		// An override must skip the hostName() round-trip entirely.
		require.Contains(t, q, "system.macros")
		return &fakeDriverRows{rows: [][]string{{"shard", "01"}}}, nil
	}}

	got, err := IntrospectNode(context.Background(), conn, "node-7")
	require.NoError(t, err)
	assert.Equal(t, NodeSpec{Name: "node-7", Macros: map[string]string{"shard": "01"}}, got)
}

func TestMacrosIntrospectNode_HostNameFromServer(t *testing.T) {
	conn := &fakeDriverConn{query: func(q string) (driver.Rows, error) {
		if strings.Contains(q, "system.macros") {
			return &fakeDriverRows{rows: [][]string{{"replica", "r1"}, {"shard", "02"}}}, nil
		}
		require.Contains(t, q, "hostName()")
		return &fakeDriverRows{rows: [][]string{{"ch-node-2"}}}, nil
	}}

	got, err := IntrospectNode(context.Background(), conn, "")
	require.NoError(t, err)
	assert.Equal(t, NodeSpec{
		Name:   "ch-node-2",
		Macros: map[string]string{"replica": "r1", "shard": "02"},
	}, got)
}

func TestMacrosIntrospectNode_MacrosErrorPropagates(t *testing.T) {
	errBoom := errors.New("boom")

	got, err := IntrospectNode(context.Background(), connReturning(nil, errBoom), "node-7")
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Equal(t, NodeSpec{}, got)
}

func TestMacrosIntrospectHostName_QueryError(t *testing.T) {
	errBoom := errors.New("boom")

	_, err := introspectHostName(context.Background(), connReturning(nil, errBoom))
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "query hostName()")
}

func TestMacrosIntrospectHostName_NoRows(t *testing.T) {
	_, err := introspectHostName(context.Background(), connReturning(&fakeDriverRows{}, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestMacrosIntrospectHostName_NoRowsIterationError(t *testing.T) {
	errBoom := errors.New("boom")
	rows := &fakeDriverRows{iterErr: errBoom}

	_, err := introspectHostName(context.Background(), connReturning(rows, nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "query hostName()")
}

func TestMacrosIntrospectHostName_ScanError(t *testing.T) {
	errBoom := errors.New("boom")
	rows := &fakeDriverRows{rows: [][]string{{"ch-node-2"}}, scanErr: errBoom}

	_, err := introspectHostName(context.Background(), connReturning(rows, nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "scan hostName()")
}
