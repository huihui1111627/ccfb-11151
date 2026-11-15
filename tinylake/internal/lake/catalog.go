package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Catalog manages tables on top of an ObjectStorage. It is stateless beyond the
// storage handle; every operation is safe for concurrent use across processes
// because linearization happens in storage-level CAS.
type Catalog struct {
	store ObjectStorage
	// procID disambiguates object names created by this process.
	procID  string
	counter atomic.Int64
	// readers tracks in-flight snapshot reads per "table@snap" so physical
	// cleanup never removes a file a running query still needs.
	readMu  sync.Mutex
	readers map[string]int
}

func NewCatalog(store ObjectStorage) *Catalog {
	return &Catalog{
		store:   store,
		procID:  fmt.Sprintf("%d-%d", osPid(), time.Now().UnixNano()%1_000_000),
		readers: map[string]int{},
	}
}

func (c *Catalog) Store() ObjectStorage { return c.store }

// newNonce returns a process-unique, monotone token for CommitID generation.
func (c *Catalog) newNonce() string {
	return fmt.Sprintf("%s-%d", c.procID, c.counter.Add(1))
}

// pointerName returns the CAS pointer key for a table.
func pointerName(table string) string { return "tbl/" + table }

func metaPrefix(table string) string { return path.Join("_lake", table, "metadata") }

// metaObject returns a GLOBALLY UNIQUE metadata object key.
//
// It must not be derived from the predicted next sequence: two concurrent
// writers both observe LastSeq=N and would otherwise target the same
// "N+1.json" key, so the loser's Put could overwrite the winner's object after
// the winner's CAS already pointed the pointer at it — silently dropping the
// winner's files. A unique name makes every metadata write immutable and
// collision-free; ordering lives inside the content (Snapshot.Seq).
func (c *Catalog) metaObject(table string) string {
	return path.Join(metaPrefix(table),
		fmt.Sprintf("%s-%d-%d.json", c.procID, time.Now().UnixNano(), c.counter.Add(1)))
}

// CreateTable bootstraps a table with an initial partition spec. Fails if the
// table pointer already exists.
func (c *Catalog) CreateTable(ctx context.Context, table string, specCols []string) (*TableMetadata, error) {
	meta := &TableMetadata{
		Table:             table,
		Specs:             []PartitionSpec{{ID: 0, Columns: append([]string(nil), specCols...)}},
		CurrentSpecID:     0,
		SnapshotRetention: 10,
		UpdatedMS:         nowMS(),
	}
	obj := c.metaObject(table)
	if err := c.putJSON(ctx, obj, meta); err != nil {
		return nil, err
	}
	if _, err := c.store.CompareAndSwap(ctx, pointerName(table), nil, []byte(obj)); err != nil {
		return nil, fmt.Errorf("create table %s: %w", table, err)
	}
	return meta, nil
}

// TableExists reports whether a table pointer is present.
func (c *Catalog) TableExists(ctx context.Context, table string) (bool, error) {
	_, err := c.currentMetaKey(ctx, table)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ListTables enumerates tables by scanning the pointer namespace.
func (c *Catalog) ListTables(ctx context.Context) ([]string, error) {
	names, err := c.store.ListPointers(ctx, "tbl")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, path.Base(n))
	}
	sort.Strings(out)
	return out, nil
}

// LoadTable reads the current metadata version of a table.
func (c *Catalog) LoadTable(ctx context.Context, table string) (*TableMetadata, error) {
	key, err := c.currentMetaKey(ctx, table)
	if err != nil {
		return nil, err
	}
	return c.readMeta(ctx, key)
}

func (c *Catalog) currentMetaKey(ctx context.Context, table string) (string, error) {
	data, err := c.store.GetPointer(ctx, pointerName(table))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c *Catalog) readMeta(ctx context.Context, key string) (*TableMetadata, error) {
	data, err := c.store.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	m := &TableMetadata{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("decode table metadata %s: %w", key, err)
	}
	return m, nil
}

func (c *Catalog) putJSON(ctx context.Context, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return c.store.Put(ctx, key, data)
}

// nextObject generates a unique object key for a data file.
func (c *Catalog) nextObject(table, suffix string) string {
	n := c.counter.Add(1)
	return path.Join(table, "data", fmt.Sprintf("%s-%d-%d%s", c.procID, time.Now().UnixNano(), n, suffix))
}
