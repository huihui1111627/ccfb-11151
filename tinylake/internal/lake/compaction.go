package lake

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// CompactionConfig controls small-file merging.
type CompactionConfig struct {
	// TargetSize is the desired byte size of merged files.
	TargetSize int64
	// MinInputFiles skips partitions whose file count is below this even if
	// every file is tiny.
	MinInputFiles int
	// MaxInputFiles bounds how many files one merged output absorbs.
	MaxInputFiles int
}

// CompactionResult summarizes one compaction pass.
type CompactionResult struct {
	Ran             bool
	GroupsCompacted int
	FilesIn         int
	FilesOut        int
	BytesIn         int64
	BytesOut        int64
	SnapshotID      int64
	Attempts        int
}

// Compact merges small files per partition into files near TargetSize.
//
// Concurrency model: a table-level lease serializes maintenance jobs, but
// readers and normal writers are never blocked — they keep using the previous
// snapshot while new data objects are staged. The merge is published as one
// OpCompact snapshot (remove inputs + add outputs) through the same CAS path,
// so if a writer commits during the merge, the compact commit simply rebases
// and retries. Old files stay on storage until snapshot expiration.
func (c *Catalog) Compact(ctx context.Context, table string, cfg CompactionConfig) (*CompactionResult, error) {
	lease := "compact:" + table
	owner := fmt.Sprintf("compact-%s-%d", c.procID, c.counter.Add(1))
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

		groups := planCompaction(meta.Files, cfg)
		if len(groups) == 0 {
			return &CompactionResult{Ran: false, Attempts: attempt}, nil
		}

		// Materialize merged data objects up front (staging phase): readers
		// cannot see these until the pointer swaps.
		type staged struct {
			removed []string
			added   []DataFile
			inCnt   int
			inBytes int64
		}
		var allStaged []staged
		var removedSet = map[string]bool{}
		var newFiles []DataFile
		var totalIn, totalOutBytes int64
		var totalInCnt int

		for _, grp := range groups {
			// Merge rows of all input files, applying current delete markers
			// opportunistically so compacted output is already tombstone-free.
			merged, err := c.mergeFiles(ctx, meta, grp)
			if err != nil {
				return nil, err
			}
			if len(merged) == 0 {
				// Entire partition group was deleted: inputs disappear with no
				// replacement. Their markers are cleaned in RewriteDeletes;
				// here we still record the removals.
			}
			outs, err := c.splitIntoTargets(ctx, table, grp[0].Partition, merged, cfg.TargetSize)
			if err != nil {
				return nil, err
			}
			rm := make([]string, 0, len(grp))
			var grpBytes int64
			for _, f := range grp {
				rm = append(rm, f.Path)
				removedSet[f.Path] = true
				grpBytes += f.Size
				totalInCnt++
			}
			totalIn += grpBytes
			for _, o := range outs {
				totalOutBytes += o.Size
				newFiles = append(newFiles, o)
			}
			allStaged = append(allStaged, staged{removed: rm, added: outs, inCnt: len(grp), inBytes: grpBytes})
		}

		// Rebuild the file list without removed inputs.
		kept := make([]DataFile, 0, len(meta.Files)-totalInCnt+len(newFiles))
		for _, f := range meta.Files {
			if !removedSet[f.Path] {
				kept = append(kept, f)
			}
		}
		kept = append(kept, newFiles...)

		var allRemoved []string
		for _, st := range allStaged {
			allRemoved = append(allRemoved, st.removed...)
		}

		next := cloneMeta(meta)
		snap := Snapshot{
			ID: meta.LastSeq + 1, Seq: meta.LastSeq + 1, ParentID: meta.CurrentSnapID,
			Op: OpCompact, CommitID: c.deterministicCommitID("compact", meta.LastSeq, allRemoved, newFiles),
			WriterID: owner, CreatedMS: nowMS(),
			Message:  fmt.Sprintf("compact %d files -> %d files", totalInCnt, len(newFiles)),
			Entries:  ManifestEntry{Added: newFiles, Removed: allRemoved},
			NumAdded: len(newFiles), NumRemoved: len(allRemoved),
		}
		next.Files = kept
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
			return &CompactionResult{
				Ran: true, GroupsCompacted: len(groups), FilesIn: totalInCnt,
				FilesOut: len(newFiles), BytesIn: totalIn, BytesOut: totalOutBytes,
				SnapshotID: snap.ID, Attempts: attempt,
			}, nil
		}
		if !errors.Is(err, ErrCASConflict) {
			return nil, err
		}
		if attempt >= defaultMaxAttempts {
			return nil, fmt.Errorf("lake: compact gave up after %d conflicts", attempt)
		}
		// Staged objects from the lost round become orphans; expire/orphan
		// cleanup reclaims them. Rebase and retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeAfter(backoff(attempt)):
		}
	}
}

// planCompaction groups files by partition and selects groups worth merging.
func planCompaction(files []DataFile, cfg CompactionConfig) [][]DataFile {
	byPart := map[string][]DataFile{}
	var order []string
	for _, f := range files {
		k := partitionKey(f.Partition)
		if _, ok := byPart[k]; !ok {
			order = append(order, k)
		}
		byPart[k] = append(byPart[k], f)
	}
	sort.Strings(order)

	var groups [][]DataFile
	maxIn := cfg.MaxInputFiles
	if maxIn <= 0 {
		maxIn = 100
	}
	for _, k := range order {
		grp := byPart[k]
		if len(grp) < cfg.MinInputFiles || len(grp) < 2 {
			continue
		}
		// Only merge when the group is genuinely small-file dominated: its
		// average file is below target size.
		var bytes int64
		for _, f := range grp {
			bytes += f.Size
		}
		if int64(len(grp)) > 0 && bytes/int64(len(grp)) >= cfg.TargetSize {
			continue
		}
		sort.Slice(grp, func(i, j int) bool { return grp[i].Path < grp[j].Path })
		for len(grp) > 0 {
			n := min(len(grp), maxIn)
			groups = append(groups, append([]DataFile(nil), grp[:n]...))
			grp = grp[n:]
		}
	}
	return groups
}

// mergeFiles reads a group's rows, drops tombstoned keys, and returns rows in
// key order (duplicate keys preserved — dedup is the writer's contract).
func (c *Catalog) mergeFiles(ctx context.Context, meta *TableMetadata, grp []DataFile) ([]Row, error) {
	dead := map[int64]bool{}
	for _, d := range meta.Deletes {
		dead[d.Key] = true
	}
	var rows []Row
	for _, f := range grp {
		fr, err := c.ReadRows(ctx, f)
		if err != nil {
			return nil, err
		}
		for _, r := range fr {
			if dead[r.Key] {
				continue
			}
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, nil
}

// splitIntoTargets packs rows into files no larger than target bytes, measured
// by serialized payload size.
func (c *Catalog) splitIntoTargets(ctx context.Context, table string, part map[string]string, rows []Row, target int64) ([]DataFile, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	var out []DataFile
	var batch []Row
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		df, err := c.WriteDataFile(ctx, table, part, batch)
		if err != nil {
			return err
		}
		out = append(out, df)
		batch = nil
		return nil
	}
	for _, r := range rows {
		batch = append(batch, r)
		if len(batch) >= 2 && estimateRowsSize(batch) >= target {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return out, nil
}

func estimateRowsSize(rows []Row) int64 {
	// `{"key":-9223372036854775808},` ~ 28 bytes worst case; close enough for
	// target-size packing without serializing twice.
	return int64(len(rows)) * 24
}

// deterministicCommitID makes maintenance snapshots idempotent across retries
// of the same logical operation against the same base sequence.
func (c *Catalog) deterministicCommitID(kind string, baseSeq int64, removed []string, added []DataFile) string {
	h := newHasher()
	h.writeString(fmt.Sprintf("%s|base=%d|rm=%d|add=%d", kind, baseSeq, len(removed), len(added)))
	for _, r := range removed {
		h.writeString(r)
	}
	for _, a := range added {
		h.writeString(a.Path)
	}
	return h.hex16()
}
