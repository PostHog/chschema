package main

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadInParallelStartsThirtyWorkersAndPreservesOrder(t *testing.T) {
	items := make([]int, 30)
	for i := range items {
		items[i] = i
	}
	started := make(chan struct{}, len(items))
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()

	type result struct {
		values []string
		err    error
	}
	var status bytes.Buffer
	done := make(chan result, 1)
	go func() {
		values, err := loadInParallel(items, parallelLoadLimit, parallelLoadProgress{
			Writer: &status,
			Prefix: "web",
			Label:  "dump models",
		}, func(item int) (string, error) {
			started <- struct{}{}
			<-release
			return fmt.Sprintf("node-%02d", item), nil
		})
		done <- result{values: values, err: err}
	}()

	for range items {
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "all 30 loaders did not start concurrently")
		}
	}
	close(release)
	released = true
	got := <-done
	require.NoError(t, got.err)
	for i, value := range got.values {
		assert.Equal(t, fmt.Sprintf("node-%02d", i), value)
	}
	assert.Contains(t, status.String(), "web: loading 30 dump models in parallel (30 workers)")
	assert.Contains(t, status.String(), "web: loading dump models: 1/30 complete")
	assert.Contains(t, status.String(), "web: loaded 30 dump models")
}

func TestLoadInParallelReturnsFirstInputError(t *testing.T) {
	var status bytes.Buffer
	_, err := loadInParallel([]int{0, 1, 2, 3}, parallelLoadLimit, parallelLoadProgress{
		Writer: &status,
		Prefix: "web",
		Label:  "models",
	}, func(item int) (int, error) {
		if item == 1 || item == 3 {
			return 0, fmt.Errorf("item %d: %w", item, errors.New("broken"))
		}
		return item, nil
	})
	require.EqualError(t, err, "item 1: broken")
	assert.Contains(t, status.String(), "web: loading models finished (2/4 failed)")
}
