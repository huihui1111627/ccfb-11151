package lake

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Row is one logical record inside a data file. The stress harness uses a
// single key; real Parquet would carry arbitrary columns.
type Row struct {
	Key int64 `json:"key"`
}

// WriteDataFile stores row payloads as a data object and returns its
// DataFile descriptor. Paths are unique per call, so concurrent writers can
// never overwrite each other's files.
func (c *Catalog) WriteDataFile(ctx context.Context, table string, part map[string]string, rows []Row) (DataFile, error) {
	key := c.nextObject(table, ".json")
	payload, err := json.Marshal(rows)
	if err != nil {
		return DataFile{}, err
	}
	if err := c.store.Put(ctx, key, payload); err != nil {
		return DataFile{}, err
	}
	minK, maxK := int64(0), int64(0)
	for i, r := range rows {
		if i == 0 || r.Key < minK {
			minK = r.Key
		}
		if i == 0 || r.Key > maxK {
			maxK = r.Key
		}
	}
	return DataFile{
		Path:      key,
		Partition: clonePartition(part),
		Size:      int64(len(payload)),
		Rows:      int64(len(rows)),
		MinKey:    minK,
		MaxKey:    maxK,
	}, nil
}

// ReadRows loads the rows of a data file.
func (c *Catalog) ReadRows(ctx context.Context, f DataFile) ([]Row, error) {
	data, err := c.store.Get(ctx, f.Path)
	if err != nil {
		return nil, err
	}
	var rows []Row
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("decode data file %s: %w", f.Path, err)
	}
	return rows, nil
}

// ScanFilter restricts a scan. All fields are optional; an empty filter scans
// the whole table.
type ScanFilter struct {
	Partitions []map[string]string // exact-match partition predicates (OR)
	KeyEquals  *int64              // point lookup
}

// ScanPlan is the result of planning: which files survive partition/key
// pruning and which delete markers apply.
type ScanPlan struct {
	Files         []DataFile
	DeleteKeys    map[int64]struct{}
	TotalFiles    int // files in the snapshot before pruning
	ScannedFiles  int // files that must be opened
	SkippedByPart int
	SkippedByKey  int
}

// PlanScan computes a read plan against the current snapshot. Old and new
// partition specs coexist transparently: a file is selected when the filter's
// partition values match on every column *that file's partition actually
// carries* (both day-only and day+hour layouts prune correctly).
func (m *TableMetadata) PlanScan(filter ScanFilter) ScanPlan {
	deleted := make(map[int64]struct{}, len(m.Deletes))
	for _, d := range m.Deletes {
		deleted[d.Key] = struct{}{}
	}

	plan := ScanPlan{TotalFiles: len(m.Files), DeleteKeys: deleted}
	files := append([]DataFile(nil), m.Files...)
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })

	for _, df := range files {
		if !partitionMatches(df.Partition, filter.Partitions) {
			plan.SkippedByPart++
			continue
		}
		if filter.KeyEquals != nil {
			k := *filter.KeyEquals
			if k < df.MinKey || k > df.MaxKey {
				plan.SkippedByKey++
				continue
			}
		}
		plan.Files = append(plan.Files, df)
	}
	plan.ScannedFiles = len(plan.Files)
	return plan
}

// Scan executes a plan, returning surviving rows in stable order. Rows whose
// key carries a delete marker are filtered out.
func (c *Catalog) Scan(ctx context.Context, m *TableMetadata, f ScanFilter) ([]Row, *ScanPlan, error) {
	plan := m.PlanScan(f)
	var out []Row
	for _, df := range plan.Files {
		rows, err := c.ReadRows(ctx, df)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range rows {
			if _, dead := plan.DeleteKeys[r.Key]; dead {
				continue
			}
			if f.KeyEquals != nil && r.Key != *f.KeyEquals {
				continue
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, &plan, nil
}

// partitionMatches reports whether a file's partition is selected by any of
// the filter's partition predicates. An empty predicate list matches all.
func partitionMatches(filePart map[string]string, predicates []map[string]string) bool {
	if len(predicates) == 0 {
		return true
	}
	for _, pred := range predicates {
		ok := true
		for k, v := range pred {
			if fv, present := filePart[k]; !present || fv != v {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}
