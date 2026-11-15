package lake

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Op is the kind of change a snapshot records.
type Op string

const (
	OpAppend  Op = "append"  // add new data files
	OpCompact Op = "compact" // rewrite files without changing logical rows
	OpRewrite Op = "rewrite" // apply deletes/updates by physically rewriting
	OpDelete  Op = "delete"  // add positional/value delete markers only
)

// DataFile is one physical file in the table.
type DataFile struct {
	Path      string            `json:"path"`
	Partition map[string]string `json:"partition"` // e.g. {"dt":"2026-11-15","hour":"10"}
	Size      int64             `json:"size"`      // bytes
	Rows      int64             `json:"rows"`
	// MinKey/MaxKey are the inclusive range of the table's key column in this
	// file, used for point-lookup pruning in the stress check.
	MinKey int64 `json:"min_key"`
	MaxKey int64 `json:"max_key"`
}

// DeleteMarker marks rows as deleted without touching the data file.
// Equality deletes reference a key value; a file is "tainted" once it contains
// any deleted key and becomes a rewrite candidate.
type DeleteMarker struct {
	Key       int64             `json:"key"`
	Partition map[string]string `json:"partition"`
	Seq       int64             `json:"seq"` // snapshot sequence that created the marker
}

// ManifestEntry is one entry in a snapshot's change list.
type ManifestEntry struct {
	Added   []DataFile     `json:"added,omitempty"`
	Removed []string       `json:"removed,omitempty"` // data file paths
	Deletes []DeleteMarker `json:"deletes,omitempty"`
}

// Snapshot is one immutable, atomic version of the table.
type Snapshot struct {
	ID        int64         `json:"id"`
	Seq       int64         `json:"seq"`
	ParentID  int64         `json:"parent_id"` // 0 means root
	Op        Op            `json:"op"`
	CommitID  string        `json:"commit_id"` // idempotency key, unique per logical write
	WriterID  string        `json:"writer_id"`
	CreatedMS int64         `json:"created_ms"`
	Message   string        `json:"message"`
	Entries   ManifestEntry `json:"entries"`
	// AddedFiles/RemovedFiles counts are denormalized for quick history display.
	NumAdded   int `json:"num_added"`
	NumRemoved int `json:"num_removed"`
}

// PartitionSpec describes how rows map to partitions. Multiple specs coexist:
// the table keeps every spec ever used so old files stay prunable.
type PartitionSpec struct {
	ID      int      `json:"id"`
	Columns []string `json:"columns"` // e.g. ["dt"] or ["dt","hour"]
}

// TableMetadata is the mutable root object; each commit writes a new immutable
// copy and swaps the table pointer to it (CAS).
type TableMetadata struct {
	Table         string          `json:"table"`
	CurrentSnapID int64           `json:"current_snapshot_id"`
	LastSeq       int64           `json:"last_seq"`
	Snapshots     []Snapshot      `json:"snapshots"`
	Specs         []PartitionSpec `json:"specs"`
	CurrentSpecID int             `json:"current_spec_id"`
	// Retention
	SnapshotRetention int `json:"snapshot_retention"` // keep last N snapshots
	// Logical-to-physical view, rebuilt per commit:
	Files   []DataFile     `json:"files"`
	Deletes []DeleteMarker `json:"deletes"`
	// UpdatedMS of this metadata version.
	UpdatedMS int64 `json:"updated_ms"`
}

func (m *TableMetadata) spec(id int) (PartitionSpec, bool) {
	for _, sp := range m.Specs {
		if sp.ID == id {
			return sp, true
		}
	}
	return PartitionSpec{}, false
}

// CurrentSpec returns the active partition spec.
func (m *TableMetadata) CurrentSpec() PartitionSpec {
	sp, _ := m.spec(m.CurrentSpecID)
	return sp
}

// findSnapshot returns a pointer to the snapshot with id (nil if absent).
func (m *TableMetadata) findSnapshot(id int64) *Snapshot {
	for i := range m.Snapshots {
		if m.Snapshots[i].ID == id {
			return &m.Snapshots[i]
		}
	}
	return nil
}

// hashCommit derives a deterministic idempotency fingerprint from a write
// request's content. Writers that retry the same logical batch produce the
// same CommitID, so the second commit becomes a no-op.
func hashCommit(table, writer string, files []DataFile, deletes []DeleteMarker, msg string) string {
	h := sha256.New()
	fmt.Fprintf(h, "table=%s|writer=%s|msg=%s|nfiles=%d|ndels=%d\n", table, writer, msg, len(files), len(deletes))
	for _, f := range files {
		fmt.Fprintf(h, "F %s %d %d %d %d %v\n", f.Path, f.Size, f.Rows, f.MinKey, f.MaxKey, f.Partition)
	}
	for _, d := range deletes {
		fmt.Fprintf(h, "D %d %d %v\n", d.Key, d.Seq, d.Partition)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// partitionKey renders a partition map as a stable string key.
func partitionKey(p map[string]string) string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(p[k])
	}
	return b.String()
}
