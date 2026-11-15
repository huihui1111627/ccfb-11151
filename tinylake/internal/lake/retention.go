package lake

import (
	"context"
	"errors"
)

// SetRetention updates the number of snapshots ExpireSnapshots keeps. Uses the
// same CAS pointer swap as commits so it is safe under concurrent writers.
func (c *Catalog) SetRetention(ctx context.Context, table string, keep int) error {
	for attempt := 1; ; attempt++ {
		if attempt > defaultMaxAttempts {
			return errors.New("lake: set retention gave up after repeated conflicts")
		}
		metaKey, err := c.currentMetaKey(ctx, table)
		if err != nil {
			return err
		}
		meta, err := c.readMeta(ctx, metaKey)
		if err != nil {
			return err
		}
		if meta.SnapshotRetention == keep {
			return nil
		}
		next := cloneMeta(meta)
		next.SnapshotRetention = keep
		next.UpdatedMS = nowMS()
		newKey := c.metaObject(table)
		if err := c.putJSON(ctx, newKey, next); err != nil {
			return err
		}
		_, err = c.store.CompareAndSwap(ctx, pointerName(table), []byte(metaKey), []byte(newKey))
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrCASConflict) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeAfter(backoff(attempt)):
		}
	}
}
