package lake

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// ErrTableNotFound is returned when a table does not exist.
var ErrTableNotFound = errors.New("lake: table not found")

// WriteRequest is one logical write: new data files and/or delete markers.
// The same logical request retried after a writer crash carries an identical
// CommitID (derived from its content), so it takes effect at most once.
type WriteRequest struct {
	WriterID string
	Message  string
	// ExplicitCommitID lets callers pin their own idempotency key (e.g. a
	// transaction id). Empty means "derive one from the content".
	ExplicitCommitID string
	Add              []DataFile
	DeleteKeys       []int64
	// DeletePartition scopes delete markers when keys are not globally unique.
	DeletePartition map[string]string
	// generatedID caches the derived CommitID so retries of the same request
	// object (e.g. a crashed writer re-driving its batch) stay idempotent.
	generatedID string
}

// CommitResult reports what a commit attempt did.
type CommitResult struct {
	SnapshotID int64
	CommitID   string
	// Duplicate is true when an earlier snapshot already carried this
	// CommitID: the write was acknowledged without creating a new snapshot.
	Duplicate bool
	// Attempts counts CAS rounds (1 = no contention).
	Attempts int
}

// default max CAS retries before giving a conflict back to the caller.
// Generous because under heavy fan-in (many goroutines, one table) a writer
// can lose many races in a row; the backoff below guarantees progress.
const defaultMaxAttempts = 100

// Commit performs an atomic, idempotent, optimistic-locked write.
//
// It takes a pointer so the CommitID derived on the first attempt is cached
// back into the request: a crashed writer that re-drives the same request
// object produces the identical id and is recognized as a duplicate.
//
// On every round it reads the latest metadata, builds the next snapshot,
// writes the new metadata object, and CAS-swaps the table pointer. A lost CAS
// means another writer committed first; the change is rebased onto the newer
// metadata and retried. Conflicts on disjoint file sets always merge.
func (c *Catalog) Commit(ctx context.Context, table string, req *WriteRequest) (*CommitResult, error) {
	commitID := req.ExplicitCommitID
	if commitID == "" {
		if req.generatedID == "" {
			// Content hash alone is NOT unique across writers (many writers
			// produce byte-identical small files). Fold in the writer id and a
			// per-process nonce so every distinct logical write gets its own
			// CommitID, while a retry of the *same request object* reuses the
			// generated id and stays idempotent.
			req.generatedID = hashCommit(table, req.WriterID+"#"+c.newNonce(), req.Add, nil, req.Message)
		}
		commitID = req.generatedID
	}

	for attempt := 1; ; attempt++ {
		if attempt > defaultMaxAttempts {
			return nil, fmt.Errorf("lake: commit %s gave up after %d CAS conflicts", commitID, attempt-1)
		}

		metaKey, err := c.currentMetaKey(ctx, table)
		if err != nil {
			return nil, err
		}
		meta, err := c.readMeta(ctx, metaKey)
		if err != nil {
			return nil, err
		}

		// Idempotency: a snapshot with this CommitID already exists. This is
		// the crash-retry / duplicate-batch path — return success, no new
		// snapshot, no duplicate data.
		if snap := meta.findSnapshotByCommit(commitID); snap != nil {
			return &CommitResult{SnapshotID: snap.ID, CommitID: commitID, Duplicate: true, Attempts: attempt}, nil
		}

		// Defensive: never add a physical data file the table already knows.
		// Two different writers using the same object path would corrupt each
		// other's view; unique object names make this unexpected.
		known := make(map[string]bool, len(meta.Files))
		for _, f := range meta.Files {
			known[f.Path] = true
		}
		added := make([]DataFile, 0, len(req.Add))
		for _, f := range req.Add {
			if known[f.Path] {
				continue // already committed (e.g. partial retry with same files)
			}
			added = append(added, f)
		}

		// Build delete markers under the next sequence number.
		var markers []DeleteMarker
		if len(req.DeleteKeys) > 0 {
			seq := meta.LastSeq + 1
			for _, k := range req.DeleteKeys {
				markers = append(markers, DeleteMarker{
					Key: k, Seq: seq, Partition: clonePartition(req.DeletePartition),
				})
			}
		}

		if len(added) == 0 && len(markers) == 0 {
			// Nothing new to apply; treat as an idempotent no-op.
			return &CommitResult{SnapshotID: meta.CurrentSnapID, CommitID: commitID, Duplicate: true, Attempts: attempt}, nil
		}

		op := OpAppend
		if len(markers) > 0 && len(added) == 0 {
			op = OpDelete
		}

		next := cloneMeta(meta)
		snap := Snapshot{
			ID:        meta.LastSeq + 1,
			Seq:       meta.LastSeq + 1,
			ParentID:  meta.CurrentSnapID,
			Op:        op,
			CommitID:  commitID,
			WriterID:  req.WriterID,
			CreatedMS: nowMS(),
			Message:   req.Message,
			Entries: ManifestEntry{
				Added:   added,
				Deletes: markers,
			},
			NumAdded: len(added),
		}
		next.Files = append(next.Files, added...)
		next.Deletes = append(next.Deletes, markers...)
		next.Snapshots = append(next.Snapshots, snap)
		next.CurrentSnapID = snap.ID
		next.LastSeq = snap.Seq
		next.UpdatedMS = nowMS()

		newKey := c.metaObject(table)
		if err := c.putJSON(ctx, newKey, next); err != nil {
			return nil, err
		}
		_, err = c.store.CompareAndSwap(ctx, pointerName(table), []byte(metaKey), []byte(newKey))
		if err == nil {
			return &CommitResult{SnapshotID: snap.ID, CommitID: commitID, Attempts: attempt}, nil
		}
		if !errors.Is(err, ErrCASConflict) {
			return nil, err
		}
		// Lost the race: jittered backoff then rebase onto the winner.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff(attempt)):
		}
	}
}

// backoff uses "equal jitter": a guaranteed-growing floor (so every loser
// backs off for real instead of repeatedly drawing ~0 and starving) plus a
// randomized half to spread contenders. Capped at 100ms.
func backoff(attempt int) time.Duration {
	base := time.Duration(2*attempt) * time.Millisecond // 2,4,6,... linear floor
	if base > 100*time.Millisecond {
		base = 100 * time.Millisecond
	}
	return base/2 + time.Duration(rand.Int63n(int64(base/2)+1))
}

func (m *TableMetadata) findSnapshotByCommit(commitID string) *Snapshot {
	for i := range m.Snapshots {
		if m.Snapshots[i].CommitID == commitID {
			return &m.Snapshots[i]
		}
	}
	return nil
}

func clonePartition(p map[string]string) map[string]string {
	if p == nil {
		return nil
	}
	out := make(map[string]string, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

func cloneMeta(m *TableMetadata) *TableMetadata {
	out := *m
	out.Specs = append([]PartitionSpec(nil), m.Specs...)
	out.Snapshots = append([]Snapshot(nil), m.Snapshots...)
	out.Files = append([]DataFile(nil), m.Files...)
	out.Deletes = append([]DeleteMarker(nil), m.Deletes...)
	return &out
}
