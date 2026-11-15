// Package lake implements a lightweight table format layer over object storage.
//
// The storage abstraction models the primitives a real object store provides:
// immutable put, get, list, delete, and a compare-and-swap on a key (pointer).
// The local-directory implementation simulates S3 semantics: writes land in a
// per-process staging area and are published with an atomic rename, so a reader
// never observes a half-written object.
package lake

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ErrNotFound is returned when an object or pointer does not exist.
var ErrNotFound = errors.New("lake: object not found")

// ErrCASConflict is returned by CompareAndSwap when the pointer's current
// value differs from the expected one.
var ErrCASConflict = errors.New("lake: cas conflict, pointer changed concurrently")

// ErrLeaseHeld is returned when a lease acquire fails because another holder
// has a live (non-expired) lease.
var ErrLeaseHeld = errors.New("lake: lease already held by another owner")

// ObjectStorage is the subset of object-storage behavior the table layer needs.
type ObjectStorage interface {
	// Put writes data at key. Overwriting an existing key is allowed for
	// non-table objects; table metadata uses unique keys, so writes are
	// effectively immutable.
	Put(ctx context.Context, key string, data []byte) error
	// Get returns the object contents.
	Get(ctx context.Context, key string) ([]byte, error)
	// Exists reports whether key exists.
	Exists(ctx context.Context, key string) (bool, error)
	// Delete removes key. Missing keys are not an error.
	Delete(ctx context.Context, key string) error
	// List returns all keys under prefix (not recursive-excluded: full listing).
	List(ctx context.Context, prefix string) ([]string, error)
	// CompareAndSwap atomically sets pointer key to newVal, but only if its
	// current value equals expect. A nil expect means "key must not exist".
	// On conflict the caller receives ErrCASConflict and the actual value.
	CompareAndSwap(ctx context.Context, key string, expect, newVal []byte) (actual []byte, err error)
	// AcquireLease takes an exclusive named lease for owner with a TTL. A
	// lease may be re-acquired by the same owner (heartbeat / renewal).
	AcquireLease(ctx context.Context, name, owner string, ttlMS int64) error
	// ReleaseLease frees a lease held by owner.
	ReleaseLease(ctx context.Context, name, owner string) error
	// GetPointer reads a CAS pointer's current value.
	GetPointer(ctx context.Context, name string) ([]byte, error)
	// ListPointers lists pointer names under prefix.
	ListPointers(ctx context.Context, prefix string) ([]string, error)
}

// ---------------- Local directory implementation ----------------

// LocalStorage simulates object storage with a directory tree.
//
// Layout:
//
//	<root>/objects/<key...>        immutable data + metadata objects
//	<root>/pointers/<name>         current pointer value (CAS targets)
//	<root>/tmp/<proc>/<rand>       staging, renamed into place atomically
//	<root>/leases/<name>           lease records
type LocalStorage struct {
	root string
	mu   sync.Mutex // serializes CAS/lease operations to make them linearizable
}

func NewLocalStorage(root string) (*LocalStorage, error) {
	for _, d := range []string{"objects", "pointers", "tmp", "leases"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fmt.Errorf("init storage dir %s: %w", d, err)
		}
	}
	return &LocalStorage{root: root}, nil
}

func (s *LocalStorage) objectPath(key string) string {
	return filepath.Join(s.root, "objects", filepath.FromSlash(key))
}

func (s *LocalStorage) pointerPath(name string) string {
	return filepath.Join(s.root, "pointers", filepath.FromSlash(name))
}

func (s *LocalStorage) leasePath(name string) string {
	return filepath.Join(s.root, "leases", filepath.FromSlash(name))
}

// stagedPut writes to a temp file first, then renames into place. os.Rename is
// atomic on a single local filesystem, matching the "new object appears at
// once" behavior readers rely on.
func (s *LocalStorage) stagedPut(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "stage-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

func (s *LocalStorage) Put(_ context.Context, key string, data []byte) error {
	if strings.Contains(key, "..") {
		return fmt.Errorf("lake: illegal key %q", key)
	}
	return s.stagedPut(s.objectPath(key), data)
}

func (s *LocalStorage) Get(_ context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(s.objectPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return data, err
}

func (s *LocalStorage) Exists(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(s.objectPath(key))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (s *LocalStorage) Delete(_ context.Context, key string) error {
	err := os.Remove(s.objectPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *LocalStorage) List(_ context.Context, prefix string) ([]string, error) {
	base := filepath.Join(s.root, "objects")
	root := filepath.Join(base, filepath.FromSlash(prefix))
	var keys []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *LocalStorage) readPointerLocked(name string) ([]byte, bool, error) {
	data, err := os.ReadFile(s.pointerPath(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (s *LocalStorage) CompareAndSwap(_ context.Context, name string, expect, newVal []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, exists, err := s.readPointerLocked(name)
	if err != nil {
		return nil, err
	}
	if expect == nil {
		if exists {
			return cur, ErrCASConflict
		}
	} else {
		if !exists || string(cur) != string(expect) {
			return cur, ErrCASConflict
		}
	}
	if err := s.stagedPut(s.pointerPath(name), newVal); err != nil {
		return nil, err
	}
	return newVal, nil
}

// leaseRecord is the JSON payload of a lease object.
type leaseRecord struct {
	Owner   string `json:"owner"`
	Expires int64  `json:"expires_ms"`
}

func (s *LocalStorage) AcquireLease(_ context.Context, name, owner string, ttlMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := nowMS()
	path := s.leasePath(name)
	data, err := os.ReadFile(path)
	rec := leaseRecord{}
	switch {
	case err == nil:
		if jerr := jsonUnmarshal(data, &rec); jerr != nil {
			return jerr
		}
		if rec.Owner != owner && rec.Expires > now {
			return fmt.Errorf("%w: owner=%s expires_in=%dms", ErrLeaseHeld, rec.Owner, rec.Expires-now)
		}
	case errors.Is(err, os.ErrNotExist):
		// free to take
	default:
		return err
	}
	rec = leaseRecord{Owner: owner, Expires: now + ttlMS}
	return s.stagedPut(path, jsonMarshalBytes(rec))
}

func (s *LocalStorage) ReleaseLease(_ context.Context, name, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.leasePath(name)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	rec := leaseRecord{}
	if err := jsonUnmarshal(data, &rec); err != nil {
		return err
	}
	if rec.Owner != owner {
		// Someone else already took over (e.g. after TTL expiry): do not
		// clobber their lease.
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *LocalStorage) GetPointer(_ context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, exists, err := s.readPointerLocked(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("%w: pointer %s", ErrNotFound, name)
	}
	return append([]byte(nil), data...), nil
}

func (s *LocalStorage) ListPointers(_ context.Context, prefix string) ([]string, error) {
	root := filepath.Join(s.root, "pointers", filepath.FromSlash(prefix))
	var names []string
	base := filepath.Join(s.root, "pointers")
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(base, p)
		if err != nil {
			return err
		}
		names = append(names, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}
