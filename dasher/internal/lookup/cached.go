package lookup

import (
	"context"
	"time"
)

// cachedLookup wraps a Lookup with a per-instance bounded LRU+TTL cache.
// Caching is the lookup's own responsibility: callers (Runner) just call
// Resolve and receive either a cached or freshly-queried result.
type cachedLookup struct {
	inner Lookup
	cache *Cache
	ttl   time.Duration
}

// NewCachedLookup wraps inner with an in-process cache using the given TTL.
// If ttl <= 0, inner is returned unwrapped (no caching).
func NewCachedLookup(inner Lookup, ttl time.Duration) Lookup {
	if ttl <= 0 {
		return inner
	}
	return &cachedLookup{
		inner: inner,
		cache: NewCache(0), // per-lookup cache, default capacity
		ttl:   ttl,
	}
}

// Resolve checks the cache first; on miss calls inner and caches single-row
// results. Multi-row and zero-row results are returned as-is without caching.
func (c *cachedLookup) Resolve(ctx context.Context, bind map[string]any) ([]Row, error) {
	key := MakeCacheKey(bind)
	if rows, ok := c.cache.Get(key); ok {
		return rows, nil
	}
	rows, err := c.inner.Resolve(ctx, bind)
	if err != nil {
		return nil, err
	}
	if len(rows) == 1 {
		c.cache.Set(key, rows, c.ttl)
	}
	return rows, nil
}
