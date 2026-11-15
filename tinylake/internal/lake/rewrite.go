package lake

import (
	"context"
	"errors"
	"fmt"
)

// RewriteResult summarizes a delete-marker physical rewrite.
type RewriteResult struct {
	Ran            bool
	FilesRewritten int
	RowsRemoved    int
	MarkersDropped int
	SnapshotID     int64
	Attempts       int
}

// RewriteDeletes physically rewrites files that contain tombstoned rows.
//
// Deletes/updates first land as cheap markers (OpDelete snapshots); once
// enough markers accumulate, this pass rewrites only the tainted files:
// dead rows are excluded, replacement files are staged, and markers whose
// keys no longer exist in any live file are dropped. The whole change is one
// OpRewrite atomic snapshot, so readers never see a half-rewritten table.
func (c *Catalog) RewriteDeletes(ctx context.Context, table string, minMarkers int) (*RewriteResult, error) {
	lease := "rewrite:" + table
	owner := fmt.Sprintf("rewrite-%s-%d", c.procID, c.counter.Add(1))
	if err := c.store.AcquireLease(ctx, lease, owner, 30_000); err != nil {
		return nil, err
	}
	defer c.store.ReleaseLease(ctx, lease, owner)

	for attempt := 1; ; attempt++ {
		metaKey, err := c.currentMetaKey(ctx, table)
		if err != nil {
			return nil, err
		}
		meta, err := c.readMeta(ctx, metaKey)
		if err != nil {
			return nil, err
		}
		if len(meta.Deletes) < minMarkers {
			return &RewriteResult{Ran: false, Attempts: attempt}, nil
		}

		dead := map[int64]bool{}
		for _, d := range meta.Deletes {
			dead[d.Key] = true
		}

		// Find tainted files and collect the set of live keys so we know
		// which markers become obsolete after the rewrite.
		type taint struct {
			file DataFile
			rows []Row
		}
		var tainted []taint
		liveKeys := map[int64]bool{}
		for _, f := range meta.Files {
			rows, err := c.ReadRows(ctx, f)
			if err != nil {
				return nil, err
			}
			hasDead := false
			for _, r := range rows {
				if dead[r.Key] {
					hasDead = true
				} else {
					liveKeys[r.Key] = true
				}
			}
			if hasDead {
				tainted = append(tainted, taint{file: f, rows: rows})
			} else {
				for _, r := range rows {
					liveKeys[r.Key] = true
				}
			}
		}
		if len(tainted) == 0 {
			// Markers reference keys already absent: drop them.
			return c.dropStaleMarkers(ctx, table, meta, metaKey, owner, attempt)
		}

		var added []DataFile
		var removed []string
		rowsRemoved := 0
		for _, t := range tainted {
			var kept []Row
			for _, r := range t.rows {
				if dead[r.Key] {
					rowsRemoved++
					continue
				}
				kept = append(kept, r)
			}
			removed = append(removed, t.file.Path)
			if len(kept) > 0 {
				df, err := c.WriteDataFile(ctx, table, t.file.Partition, kept)
				if err != nil {
					return nil, err
				}
				added = append(added, df)
				for _, r := range kept {
					liveKeys[r.Key] = true
				}
			}
		}

		// A marker is obsolete when its key is not present in any live row.
		var keptMarkers, dropped []DeleteMarker
		for _, d := range meta.Deletes {
			if liveKeys[d.Key] {
				keptMarkers = append(keptMarkers, d)
			} else {
				dropped = append(dropped, d)
			}
		}

		removedSet := map[string]bool{}
		for _, p := range removed {
			removedSet[p] = true
		}
		newFileList := make([]DataFile, 0, len(meta.Files)-len(removed)+len(added))
		for _, f := range meta.Files {
			if !removedSet[f.Path] {
				newFileList = append(newFileList, f)
			}
		}
		newFileList = append(newFileList, added...)

		next := cloneMeta(meta)
		snap := Snapshot{
			ID: meta.LastSeq + 1, Seq: meta.LastSeq + 1, ParentID: meta.CurrentSnapID,
			Op: OpRewrite, CommitID: c.deterministicCommitID("rewrite", meta.LastSeq, removed, added),
			WriterID: owner, CreatedMS: nowMS(),
			Message: fmt.Sprintf("rewrite %d tainted files, %d rows removed, %d markers dropped",
				len(tainted), rowsRemoved, len(dropped)),
			Entries:  ManifestEntry{Added: added, Removed: removed, Deletes: negateMarkers(dropped)},
			NumAdded: len(added), NumRemoved: len(removed),
		}
		next.Files = newFileList
		next.Deletes = keptMarkers
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
			return &RewriteResult{Ran: true, FilesRewritten: len(tainted), RowsRemoved: rowsRemoved,
				MarkersDropped: len(dropped), SnapshotID: snap.ID, Attempts: attempt}, nil
		}
		if !errors.Is(err, ErrCASConflict) {
			return nil, err
		}
		if attempt >= defaultMaxAttempts {
			return nil, fmt.Errorf("lake: rewrite gave up after %d conflicts", attempt)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeAfter(backoff(attempt)):
		}
	}
}

// dropStaleMarkers handles the degenerate case where every marker already
// points at absent keys.
func (c *Catalog) dropStaleMarkers(ctx context.Context, table string, meta *TableMetadata, metaKey, owner string, attempt int) (*RewriteResult, error) {
	dropped := append([]DeleteMarker(nil), meta.Deletes...)
	next := cloneMeta(meta)
	next.Deletes = nil
	snap := Snapshot{
		ID: meta.LastSeq + 1, Seq: meta.LastSeq + 1, ParentID: meta.CurrentSnapID,
		Op: OpRewrite, CommitID: c.deterministicCommitID("rewrite-drop", meta.LastSeq, nil, nil),
		WriterID: owner, CreatedMS: nowMS(),
		Message: fmt.Sprintf("drop %d stale markers", len(dropped)),
		Entries: ManifestEntry{Deletes: negateMarkers(dropped)},
	}
	next.Snapshots = append(next.Snapshots, snap)
	next.CurrentSnapID = snap.ID
	next.LastSeq = snap.Seq
	next.UpdatedMS = nowMS()

	newKey := c.metaObject(table)
	if err := c.putJSON(ctx, newKey, next); err != nil {
		return nil, err
	}
	if _, err := c.store.CompareAndSwap(ctx, pointerName(table), []byte(metaKey), []byte(newKey)); err != nil {
		return nil, err
	}
	return &RewriteResult{Ran: true, MarkersDropped: len(dropped), SnapshotID: snap.ID, Attempts: attempt}, nil
}

// negateMarkers records marker removals in a manifest. Reusing DeleteMarker
// with a negative seq signals "drop this key" on replay.
func negateMarkers(ds []DeleteMarker) []DeleteMarker {
	out := make([]DeleteMarker, len(ds))
	for i, d := range ds {
		d.Seq = -d.Seq
		out[i] = d
	}
	return out
}

// AddPartitionSpec introduces a new partition spec (e.g. day -> day+hour) and
// makes it current. Existing files keep their old spec's partition values;
// both layouts coexist and prune correctly because partition matching only
// constrains columns a file actually carries.
func (c *Catalog) AddPartitionSpec(ctx context.Context, table string, cols []string) (int, error) {
	for attempt := 1; ; attempt++ {
		if attempt > defaultMaxAttempts {
			return 0, fmt.Errorf("lake: add partition spec gave up after %d conflicts", attempt-1)
		}
		metaKey, err := c.currentMetaKey(ctx, table)
		if err != nil {
			return 0, err
		}
		meta, err := c.readMeta(ctx, metaKey)
		if err != nil {
			return 0, err
		}
		for _, sp := range meta.Specs {
			if equalStringSlice(sp.Columns, cols) {
				if sp.ID == meta.CurrentSpecID {
					return sp.ID, nil
				}
				id, err := c.setCurrentSpec(ctx, table, meta, metaKey, sp.ID)
				if errors.Is(err, ErrCASConflict) {
					break // re-read and retry
				}
				return id, err
			}
		}
		newID := 0
		for _, sp := range meta.Specs {
			if sp.ID >= newID {
				newID = sp.ID + 1
			}
		}
		id, err := c.setCurrentSpec(ctx, table, meta, metaKey, newID,
			PartitionSpec{ID: newID, Columns: append([]string(nil), cols...)})
		if errors.Is(err, ErrCASConflict) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-timeAfter(backoff(attempt)):
			}
			continue
		}
		return id, err
	}
}

func (c *Catalog) setCurrentSpec(ctx context.Context, table string, meta *TableMetadata, metaKey string, newID int, extra ...PartitionSpec) (int, error) {
	next := cloneMeta(meta)
	next.Specs = append(next.Specs, extra...)
	next.CurrentSpecID = newID
	next.UpdatedMS = nowMS()
	newKey := c.metaObject(table)
	if err := c.putJSON(ctx, newKey, next); err != nil {
		return 0, err
	}
	if _, err := c.store.CompareAndSwap(ctx, pointerName(table), []byte(metaKey), []byte(newKey)); err != nil {
		return 0, err
	}
	return newID, nil
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
