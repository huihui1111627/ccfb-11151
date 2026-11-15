package lake

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
)

// ExpireResult summarizes snapshot expiration and orphan cleanup.
type ExpireResult struct {
	SnapshotsExpired int
	DataFilesDeleted int
	MetaFilesDeleted int
	OrphansDeleted   int
	OrphansKept      []string // not old enough to safely delete
	ProtectedReads   int
}

// SnapshotReader is the guard for in-flight reads. A reader registers before
// scanning and unregisters when done; cleanup never deletes data files that
// any registered reader's snapshot still references. This models the
// "snapshot is pinned while someone reads it" contract.
type SnapshotReader struct {
	cat    *Catalog
	table  string
	snapID int64
	active bool
}

// BeginRead pins the current snapshot for the duration of a scan.
func (c *Catalog) BeginRead(ctx context.Context, table string) (*SnapshotReader, error) {
	meta, err := c.LoadTable(ctx, table)
	if err != nil {
		return nil, err
	}
	r := &SnapshotReader{cat: c, table: table, snapID: meta.CurrentSnapID, active: true}
	c.readMu.Lock()
	c.readers[keyOf(table, meta.CurrentSnapID)]++
	c.readMu.Unlock()
	return r, nil
}

func (r *SnapshotReader) SnapshotID() int64 { return r.snapID }

// BeginReadAt pins a specific snapshot id, used to prove an old snapshot's
// files survive expiration while a reader holds it.
func (c *Catalog) BeginReadAt(ctx context.Context, table string, snapID int64) (*SnapshotReader, error) {
	if _, err := c.LoadTable(ctx, table); err != nil {
		return nil, err
	}
	r := &SnapshotReader{cat: c, table: table, snapID: snapID, active: true}
	c.readMu.Lock()
	c.readers[keyOf(table, snapID)]++
	c.readMu.Unlock()
	return r, nil
}

// EndRead releases the pin. Safe to call multiple times.
func (r *SnapshotReader) EndRead() {
	if !r.active {
		return
	}
	r.cat.readMu.Lock()
	k := keyOf(r.table, r.snapID)
	if r.cat.readers[k] > 0 {
		r.cat.readers[k]--
		if r.cat.readers[k] == 0 {
			delete(r.cat.readers, k)
		}
	}
	r.cat.readMu.Unlock()
	r.active = false
}

func keyOf(table string, snapID int64) string { return fmt.Sprintf("%s@%d", table, snapID) }

// protectedFiles computes the set of data file paths that must not be
// deleted: files referenced by the current snapshot plus every snapshot with
// a registered reader.
func (c *Catalog) protectedFiles(ctx context.Context, meta *TableMetadata) (map[string]bool, int, error) {
	protected := map[string]bool{}
	for _, f := range meta.Files {
		protected[f.Path] = true
	}
	pinned := 0
	c.readMu.Lock()
	for k := range c.readers {
		var table string
		var snapID int64
		if _, err := fmt.Sscanf(k, "%[^@]@%d", &table, &snapID); err != nil {
			continue
		}
		if table != meta.Table {
			continue
		}
		if snapID == meta.CurrentSnapID {
			continue // already covered
		}
		// Reconstruct that old snapshot's file set by replaying history.
		files, ok := c.snapshotFileSet(ctx, meta, snapID)
		if !ok {
			continue
		}
		for p := range files {
			protected[p] = true
		}
		pinned++
	}
	c.readMu.Unlock()
	return protected, pinned, nil
}

// snapshotFileSet replays snapshots up to and including snapID to derive its
// physical file set.
func (c *Catalog) snapshotFileSet(_ context.Context, meta *TableMetadata, snapID int64) (map[string]bool, bool) {
	snaps := append([]Snapshot(nil), meta.Snapshots...)
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Seq < snaps[j].Seq })
	files := map[string]bool{}
	found := false
	for _, s := range snaps {
		for _, f := range s.Entries.Added {
			files[f.Path] = true
		}
		for _, p := range s.Entries.Removed {
			delete(files, p)
		}
		if s.ID == snapID {
			found = true
			break
		}
	}
	if !found {
		return nil, false
	}
	return files, true
}

// ExpireSnapshots removes all but the last `retention` snapshots from
// metadata, then physically deletes only data files that are both (a) absent
// from every retained/pinned snapshot and (b) not protected by an in-flight
// reader.
func (c *Catalog) ExpireSnapshots(ctx context.Context, table string) (*ExpireResult, error) {
	res := &ExpireResult{}
	for attempt := 1; ; attempt++ {
		metaKey, err := c.currentMetaKey(ctx, table)
		if err != nil {
			return nil, err
		}
		meta, err := c.readMeta(ctx, metaKey)
		if err != nil {
			return nil, err
		}
		retention := meta.SnapshotRetention
		if retention < 1 {
			retention = 1
		}
		if len(meta.Snapshots) <= retention {
			// Still run orphan cleanup even when nothing expires.
			orphans, kept, err := c.cleanupOrphans(ctx, meta)
			if err != nil {
				return nil, err
			}
			res.OrphansDeleted, res.OrphansKept = orphans, kept
			return res, nil
		}

		protected, pinned, err := c.protectedFiles(ctx, meta)
		if err != nil {
			return nil, err
		}
		res.ProtectedReads = pinned

		expired := meta.Snapshots[:len(meta.Snapshots)-retention]
		retained := meta.Snapshots[len(meta.Snapshots)-retention:]

		// Files still referenced by retained snapshots must survive.
		retainedFiles := map[string]bool{}
		// Replay retained snapshots (union of their file sets) so a file
		// still referenced by any retained snapshot is never deleted.
		unionRetained := replayUnion(meta.Snapshots, retained)
		for p := range unionRetained {
			retainedFiles[p] = true
		}

		// Deletable = files added/removed only within expired history and not
		// protected by readers.
		expiredTouch := map[string]bool{}
		for _, s := range expired {
			for _, f := range s.Entries.Added {
				expiredTouch[f.Path] = true
			}
			for _, p := range s.Entries.Removed {
				expiredTouch[p] = true
			}
		}
		var toDelete []string
		for p := range expiredTouch {
			if retainedFiles[p] || protected[p] {
				continue
			}
			toDelete = append(toDelete, p)
		}
		sort.Strings(toDelete)

		// Publish the trimmed metadata first; only then remove objects, so a
		// crash between the two steps leaves harmless orphans rather than
		// dangling references.
		next := cloneMeta(meta)
		next.Snapshots = append([]Snapshot(nil), retained...)
		next.UpdatedMS = nowMS()
		newKey := c.metaObject(table)
		if err := c.putJSON(ctx, newKey, next); err != nil {
			return nil, err
		}
		if _, err := c.store.CompareAndSwap(ctx, pointerName(table), []byte(metaKey), []byte(newKey)); err != nil {
			if errors.Is(err, ErrCASConflict) {
				if attempt >= defaultMaxAttempts {
					return nil, fmt.Errorf("lake: expire gave up after %d conflicts", attempt)
				}
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-timeAfter(backoff(attempt)):
				}
				continue
			}
			return nil, err
		}

		deleted := 0
		for _, p := range toDelete {
			if err := c.store.Delete(ctx, p); err != nil {
				return nil, err
			}
			deleted++
		}
		// Old metadata objects beyond retention are also reclaimed.
		metaDeleted, err := c.cleanupOldMetaObjects(ctx, table, next)
		if err != nil {
			return nil, err
		}
		orphans, kept, err := c.cleanupOrphans(ctx, next)
		if err != nil {
			return nil, err
		}
		res.SnapshotsExpired = len(expired)
		res.DataFilesDeleted = deleted
		res.MetaFilesDeleted = metaDeleted
		res.OrphansDeleted = orphans
		res.OrphansKept = kept
		return res, nil
	}
}

// replayUnion returns the union of file sets held by the chosen snapshots,
// replaying the full history in sequence order.
func replayUnion(all, chosen []Snapshot) map[string]bool {
	want := map[int64]bool{}
	for _, s := range chosen {
		want[s.ID] = true
	}
	union := map[string]bool{}
	cur := map[string]bool{}
	snaps := append([]Snapshot(nil), all...)
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Seq < snaps[j].Seq })
	for _, s := range snaps {
		for _, f := range s.Entries.Added {
			cur[f.Path] = true
		}
		for _, p := range s.Entries.Removed {
			delete(cur, p)
		}
		if want[s.ID] {
			for p := range cur {
				union[p] = true
			}
		}
	}
	return union
}

// cleanupOldMetaObjects deletes metadata objects whose newest snapshot is
// older than the oldest retained snapshot, always keeping the currently
// referenced object. Metadata keys are unique (non-sequential), so age is
// read from the object's content rather than its filename.
func (c *Catalog) cleanupOldMetaObjects(ctx context.Context, table string, meta *TableMetadata) (int, error) {
	keys, err := c.store.List(ctx, metaPrefix(table))
	if err != nil {
		return 0, err
	}
	oldestSeq := int64(1) << 62
	for _, s := range meta.Snapshots {
		if s.Seq < oldestSeq {
			oldestSeq = s.Seq
		}
	}
	current, err := c.currentMetaKey(ctx, table)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, k := range keys {
		if k == current {
			continue
		}
		old, err := c.readMeta(ctx, k)
		if err != nil {
			// Unreadable/non-metadata object: leave it for a future pass.
			continue
		}
		// Only reclaim versions strictly older than the retained history; an
		// object whose LastSeq is still in the retained range may be referenced
		// by a reader pinned to an older snapshot.
		if old.LastSeq < oldestSeq {
			if err := c.store.Delete(ctx, k); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}

// orphanMinAgeMS: an unreferenced data object younger than this may be a
// staging file of an in-flight commit/compaction and must be spared.
const orphanMinAgeMS = 5_000

// cleanupOrphans deletes data objects under the table prefix that no snapshot
// references and that are older than the safety grace period.
func (c *Catalog) cleanupOrphans(ctx context.Context, meta *TableMetadata) (int, []string, error) {
	referenced := map[string]bool{}
	for _, f := range meta.Files {
		referenced[f.Path] = true
	}
	keys, err := c.store.List(ctx, path.Join(meta.Table, "data"))
	if err != nil {
		return 0, nil, err
	}
	cutoff := nowMS() - orphanMinAgeMS
	deleted := 0
	var kept []string
	for _, k := range keys {
		if referenced[k] {
			continue
		}
		modMS, ok := c.objectAgeMS(ctx, k)
		if !ok || modMS > cutoff {
			// Can't determine age, or object too young: it may be a staging
			// file of an in-flight operation, so keep it.
			kept = append(kept, k)
			continue
		}
		if err := c.store.Delete(ctx, k); err != nil {
			return deleted, kept, err
		}
		deleted++
	}
	return deleted, kept, nil
}

// objectAgeMS returns the object's modification timestamp (ms). The local
// implementation uses file mtime; a real backend would use object metadata.
func (c *Catalog) objectAgeMS(_ context.Context, key string) (int64, bool) {
	ls, ok := c.store.(*LocalStorage)
	if !ok {
		return 0, false
	}
	info, err := osStat(ls.objectPath(key))
	if err != nil {
		return 0, false
	}
	return info.ModTime().UnixMilli(), true
}
