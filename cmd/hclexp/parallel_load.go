package main

import (
	"fmt"
	"io"
	"sync"
)

const parallelLoadLimit = 32

// parallelLoadProgress describes optional human-readable progress. Writer is
// normally os.Stderr at a CLI boundary and nil for library-style callers and
// tests that do not want status output.
type parallelLoadProgress struct {
	Writer io.Writer
	Prefix string
	Label  string
}

// loadInParallel applies loader through a bounded worker pool. Results and
// errors retain input order regardless of completion order; when several loads
// fail, the caller receives the first error in deterministic input order.
func loadInParallel[T, R any](items []T, parallelism int, progress parallelLoadProgress, loader func(T) (R, error)) ([]R, error) {
	if len(items) == 0 {
		return []R{}, nil
	}
	if parallelism < 1 {
		parallelism = 1
	}
	if parallelism > len(items) {
		parallelism = len(items)
	}

	reporter := newParallelLoadReporter(progress, len(items), parallelism)
	results := make([]R, len(items))
	errs := make([]error, len(items))
	jobs := make(chan int, len(items))
	for i := range items {
		jobs <- i
	}
	close(jobs)

	var workers sync.WaitGroup
	workers.Add(parallelism)
	for range parallelism {
		go func() {
			defer workers.Done()
			for i := range jobs {
				results[i], errs[i] = loader(items[i])
				reporter.complete(errs[i])
			}
		}()
	}
	workers.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

type parallelLoadReporter struct {
	progress  parallelLoadProgress
	total     int
	every     int
	next      int
	completed int
	failed    int
	mu        sync.Mutex
}

func newParallelLoadReporter(progress parallelLoadProgress, total, workers int) *parallelLoadReporter {
	r := &parallelLoadReporter{
		progress: progress,
		total:    total,
		every:    max(1, (total+9)/10),
	}
	r.next = r.every
	if progress.Writer != nil && total > 1 {
		fmt.Fprintf(progress.Writer, "%s: loading %d %s in parallel (%d workers)\n",
			progress.Prefix, total, progress.Label, workers)
	}
	return r
}

func (r *parallelLoadReporter) complete(err error) {
	if r.progress.Writer == nil || r.total <= 1 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed++
	if err != nil {
		r.failed++
	}
	if r.completed == r.total {
		if r.failed == 0 {
			fmt.Fprintf(r.progress.Writer, "%s: loaded %d %s\n", r.progress.Prefix, r.total, r.progress.Label)
		} else {
			fmt.Fprintf(r.progress.Writer, "%s: loading %s finished (%d/%d failed)\n",
				r.progress.Prefix, r.progress.Label, r.failed, r.total)
		}
		return
	}
	if r.completed != 1 && r.completed < r.next {
		return
	}
	for r.next <= r.completed {
		r.next += r.every
	}
	fmt.Fprintf(r.progress.Writer, "%s: loading %s: %d/%d complete\n",
		r.progress.Prefix, r.progress.Label, r.completed, r.total)
}
