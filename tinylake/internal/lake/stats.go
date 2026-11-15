package lake

import (
	"sort"
)

// PartitionStat describes file layout inside one partition.
type PartitionStat struct {
	Partition map[string]string `json:"partition"`
	Files     int               `json:"files"`
	Rows      int64             `json:"rows"`
	Bytes     int64             `json:"bytes"`
	AvgBytes  int64             `json:"avg_bytes"`
}

// TableStat is the full statistics payload shown on the console.
type TableStat struct {
	Table         string          `json:"table"`
	SnapshotCount int             `json:"snapshot_count"`
	CurrentSeq    int64           `json:"current_seq"`
	FileCount     int             `json:"file_count"`
	TotalRows     int64           `json:"total_rows"`
	TotalBytes    int64           `json:"total_bytes"`
	AvgFileBytes  int64           `json:"avg_file_bytes"`
	DeleteMarkers int             `json:"delete_markers"`
	Partitions    []PartitionStat `json:"partitions"`
	// FullScanFiles is what a query with no filters must open (= all live
	// files). Filtered examples demonstrate pruning.
	FullScanFiles int             `json:"full_scan_files"`
	Specs         []PartitionSpec `json:"specs"`
	CurrentSpecID int             `json:"current_spec_id"`
}

// Stats computes table statistics, including per-partition distribution and
// the file counts several representative scans would touch.
func (m *TableMetadata) Stats() TableStat {
	st := TableStat{
		Table:         m.Table,
		SnapshotCount: len(m.Snapshots),
		CurrentSeq:    m.LastSeq,
		FileCount:     len(m.Files),
		DeleteMarkers: len(m.Deletes),
		FullScanFiles: len(m.Files),
		Specs:         append([]PartitionSpec(nil), m.Specs...),
		CurrentSpecID: m.CurrentSpecID,
	}
	byPart := map[string]*PartitionStat{}
	var order []string
	for _, f := range m.Files {
		k := partitionKey(f.Partition)
		p, ok := byPart[k]
		if !ok {
			p = &PartitionStat{Partition: clonePartition(f.Partition)}
			byPart[k] = p
			order = append(order, k)
		}
		p.Files++
		p.Rows += f.Rows
		p.Bytes += f.Size
		st.TotalRows += f.Rows
		st.TotalBytes += f.Size
	}
	sort.Strings(order)
	for _, k := range order {
		p := byPart[k]
		if p.Files > 0 {
			p.AvgBytes = p.Bytes / int64(p.Files)
		}
		st.Partitions = append(st.Partitions, *p)
	}
	if st.FileCount > 0 {
		st.AvgFileBytes = st.TotalBytes / int64(st.FileCount)
	}
	return st
}

// ScanFileCount is a lightweight counter for "how many files would this
// filter touch" without reading any row data.
func (m *TableMetadata) ScanFileCount(filter ScanFilter) int {
	return m.PlanScan(filter).ScannedFiles
}
