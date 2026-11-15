package lake

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func newTestCatalog(t *testing.T) (*Catalog, context.Context) {
	t.Helper()
	st, err := NewLocalStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return NewCatalog(st), context.Background()
}

func rows(keys ...int64) []Row {
	out := make([]Row, len(keys))
	for i, k := range keys {
		out[i] = Row{Key: k}
	}
	return out
}

// TestConcurrentCommitNoLossNoDup is the core ACID check: many goroutines
// commit disjoint key ranges; the final table must contain every key once.
func TestConcurrentCommitNoLossNoDup(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	if _, err := cat.CreateTable(ctx, "t", []string{"dt"}); err != nil {
		t.Fatal(err)
	}
	const writers, rounds, per = 20, 10, 8
	var key int64
	var keyMu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				ks := make([]int64, per)
				keyMu.Lock()
				for i := range ks {
					key++
					ks[i] = key
				}
				keyMu.Unlock()
				df, err := cat.WriteDataFile(ctx, "t", map[string]string{"dt": "d"}, rows(ks...))
				if err != nil {
					t.Error(err)
					return
				}
				req := &WriteRequest{WriterID: fmt.Sprintf("w-%d", w), Add: []DataFile{df}}
				if _, err := cat.Commit(ctx, "t", req); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	meta, err := cat.LoadTable(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := cat.Scan(ctx, meta, ScanFilter{})
	if err != nil {
		t.Fatal(err)
	}
	want := int64(writers * rounds * per)
	if int64(len(got)) != want {
		t.Fatalf("row count = %d, want %d", len(got), want)
	}
	seen := map[int64]bool{}
	for _, r := range got {
		if seen[r.Key] {
			t.Fatalf("duplicate key %d", r.Key)
		}
		seen[r.Key] = true
	}
	for k := int64(1); k <= want; k++ {
		if !seen[k] {
			t.Fatalf("missing key %d", k)
		}
	}
}

// TestIdempotentRetry covers both content-derived and explicit CommitIDs.
func TestIdempotentRetry(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	if _, err := cat.CreateTable(ctx, "t", nil); err != nil {
		t.Fatal(err)
	}
	df, err := cat.WriteDataFile(ctx, "t", nil, rows(1, 2, 3))
	if err != nil {
		t.Fatal(err)
	}
	req := &WriteRequest{WriterID: "w", Add: []DataFile{df}}
	r1, err := cat.Commit(ctx, "t", req)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := cat.Commit(ctx, "t", req) // same request object -> same generated id
	if err != nil {
		t.Fatal(err)
	}
	if r1.Duplicate || !r2.Duplicate {
		t.Fatalf("retry dedup wrong: r1=%v r2=%v", r1.Duplicate, r2.Duplicate)
	}
	meta, _ := cat.LoadTable(ctx, "t")
	if len(meta.Snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(meta.Snapshots))
	}

	// Explicit id, submitted three times.
	df2, _ := cat.WriteDataFile(ctx, "t", nil, rows(4, 5))
	fixed := &WriteRequest{WriterID: "w", ExplicitCommitID: "txn-xyz", Add: []DataFile{df2}}
	for i := 0; i < 3; i++ {
		if _, err := cat.Commit(ctx, "t", fixed); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ = cat.LoadTable(ctx, "t")
	if len(meta.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(meta.Snapshots))
	}
	got, _, _ := cat.Scan(ctx, meta, ScanFilter{})
	if len(got) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(got))
	}
}

// TestDistinctWritersNoCommitIDCollision ensures identical files from
// different writers do not collapse into one idempotent commit.
func TestDistinctWritersNoCommitIDCollision(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	if _, err := cat.CreateTable(ctx, "t", nil); err != nil {
		t.Fatal(err)
	}
	for w := 0; w < 5; w++ {
		// Every file carries the same rows/shape.
		df, err := cat.WriteDataFile(ctx, "t", nil, rows(1, 2))
		if err != nil {
			t.Fatal(err)
		}
		req := &WriteRequest{WriterID: fmt.Sprintf("w%d", w), Add: []DataFile{df}}
		if _, err := cat.Commit(ctx, "t", req); err != nil {
			t.Fatal(err)
		}
	}
	meta, _ := cat.LoadTable(ctx, "t")
	if len(meta.Snapshots) != 5 {
		t.Fatalf("expected 5 distinct snapshots, got %d", len(meta.Snapshots))
	}
}

// TestPartitionEvolutionPruning checks day-only and day+hour files coexist
// and prune on overlapping columns.
func TestPartitionEvolutionPruning(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	if _, err := cat.CreateTable(ctx, "t", []string{"dt"}); err != nil {
		t.Fatal(err)
	}
	old, _ := cat.WriteDataFile(ctx, "t", map[string]string{"dt": "d1"}, rows(1))
	if _, err := cat.Commit(ctx, "t", &WriteRequest{WriterID: "a", Add: []DataFile{old}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.AddPartitionSpec(ctx, "t", []string{"dt", "hour"}); err != nil {
		t.Fatal(err)
	}
	h1, _ := cat.WriteDataFile(ctx, "t", map[string]string{"dt": "d1", "hour": "01"}, rows(2))
	h2, _ := cat.WriteDataFile(ctx, "t", map[string]string{"dt": "d1", "hour": "02"}, rows(3))
	if _, err := cat.Commit(ctx, "t", &WriteRequest{WriterID: "b", Add: []DataFile{h1, h2}}); err != nil {
		t.Fatal(err)
	}
	meta, _ := cat.LoadTable(ctx, "t")
	if got := meta.ScanFileCount(ScanFilter{Partitions: []map[string]string{{"dt": "d1"}}}); got != 3 {
		t.Fatalf("day filter should hit all 3 files, got %d", got)
	}
	if got := meta.ScanFileCount(ScanFilter{Partitions: []map[string]string{{"dt": "d1", "hour": "02"}}}); got != 1 {
		t.Fatalf("hour filter should hit 1 file, got %d", got)
	}
	if got := meta.ScanFileCount(ScanFilter{Partitions: []map[string]string{{"dt": "other"}}}); got != 0 {
		t.Fatalf("unmatched day should hit 0, got %d", got)
	}
}

// TestDeleteMarkerThenRewrite verifies tombstones filter reads and a physical
// rewrite drops the dead rows and obsolete markers.
func TestDeleteMarkerThenRewrite(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	if _, err := cat.CreateTable(ctx, "t", nil); err != nil {
		t.Fatal(err)
	}
	df, _ := cat.WriteDataFile(ctx, "t", nil, rows(1, 2, 3, 4))
	if _, err := cat.Commit(ctx, "t", &WriteRequest{WriterID: "w", Add: []DataFile{df}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Commit(ctx, "t", &WriteRequest{WriterID: "d", DeleteKeys: []int64{2, 4}}); err != nil {
		t.Fatal(err)
	}
	meta, _ := cat.LoadTable(ctx, "t")
	got, _, _ := cat.Scan(ctx, meta, ScanFilter{})
	if len(got) != 2 || got[0].Key != 1 || got[1].Key != 3 {
		t.Fatalf("marker filter wrong: %+v", got)
	}
	rw, err := cat.RewriteDeletes(ctx, "t", 1)
	if err != nil || !rw.Ran {
		t.Fatalf("rewrite: %+v err=%v", rw, err)
	}
	meta, _ = cat.LoadTable(ctx, "t")
	if len(meta.Deletes) != 0 {
		t.Fatalf("markers should be cleared, got %d", len(meta.Deletes))
	}
	got, _, _ = cat.Scan(ctx, meta, ScanFilter{})
	if len(got) != 2 {
		t.Fatalf("expected 2 surviving rows, got %d", len(got))
	}
}

// TestExpireProtectsInFlightReader ensures cleanup never removes a file still
// referenced by a pinned snapshot.
func TestExpireProtectsInFlightReader(t *testing.T) {
	cat, ctx := newTestCatalog(t)
	st := cat.store
	if _, err := cat.CreateTable(ctx, "t", nil); err != nil {
		t.Fatal(err)
	}
	var firstPath string
	for i := 0; i < 4; i++ {
		df, _ := cat.WriteDataFile(ctx, "t", nil, rows(int64(i*10+1), int64(i*10+2)))
		if i == 0 {
			firstPath = df.Path
		}
		if _, err := cat.Commit(ctx, "t", &WriteRequest{WriterID: "w", Add: []DataFile{df}}); err != nil {
			t.Fatal(err)
		}
	}
	// Compact so the first file gets removed from the head snapshot.
	if _, err := cat.Compact(ctx, "t", CompactionConfig{TargetSize: 1, MinInputFiles: 2, MaxInputFiles: 10}); err != nil {
		t.Fatal(err)
	}
	// Pin the ORIGINAL snapshot (seq 1) whose only file is firstPath.
	pin, err := cat.BeginReadAt(ctx, "t", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.SetRetention(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.ExpireSnapshots(ctx, "t"); err != nil {
		t.Fatal(err)
	}
	exists, err := st.Exists(ctx, firstPath)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("pinned reader's file was deleted during expiration")
	}
	pin.EndRead()
	// After releasing the pin, a second expiration may reclaim it.
	if _, err := cat.ExpireSnapshots(ctx, "t"); err != nil {
		t.Fatal(err)
	}
}
