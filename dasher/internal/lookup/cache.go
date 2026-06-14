package lookup

import (
	"container/list"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

const (
	defaultMaxEntries = 1024
	jitterFraction    = 0.15 // ±15% TTL jitter
)

type cacheEntry struct {
	key       string
	value     []Row
	expiresAt time.Time
}

// Cache is a bounded in-process LRU cache with per-entry TTL and jitter.
// Safe for concurrent use.
type Cache struct {
	mu         sync.Mutex
	maxEntries int
	items      map[string]*list.Element
	lru        *list.List // front = most recently used
}

// NewCache creates a Cache with the given capacity. If maxEntries <= 0,
// defaultMaxEntries is used.
func NewCache(maxEntries int) *Cache {
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &Cache{
		maxEntries: maxEntries,
		items:      make(map[string]*list.Element),
		lru:        list.New(),
	}
}

// MakeCacheKey builds a stable string key from a bind map.
// The lookup name is not included — the cache is per-lookup instance.
func MakeCacheKey(bind map[string]any) string {
	keys := make([]string, 0, len(bind))
	for k := range bind {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var key string
	for _, k := range keys {
		key += fmt.Sprintf("|%s=%T:%v", k, bind[k], bind[k])
	}
	return key
}

// Get returns (rows, true) if the key is in the cache and not expired.
func (c *Cache) Get(key string) ([]Row, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	entry := el.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		c.lru.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.lru.MoveToFront(el)
	return entry.value, true
}

// Set stores rows under key with a jittered TTL derived from ttl.
// If ttl <= 0, the entry is not cached.
func (c *Cache) Set(key string, value []Row, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	jitter := time.Duration(float64(ttl) * jitterFraction * (rand.Float64()*2 - 1))
	effectiveTTL := ttl + jitter
	if effectiveTTL <= 0 {
		effectiveTTL = ttl
	}
	exp := time.Now().Add(effectiveTTL)

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.lru.MoveToFront(el)
		el.Value.(*cacheEntry).value = value
		el.Value.(*cacheEntry).expiresAt = exp
		return
	}

	if c.lru.Len() >= c.maxEntries {
		// Evict LRU
		back := c.lru.Back()
		if back != nil {
			evicted := c.lru.Remove(back).(*cacheEntry)
			delete(c.items, evicted.key)
		}
	}
	entry := &cacheEntry{key: key, value: value, expiresAt: exp}
	el := c.lru.PushFront(entry)
	c.items[key] = el
}

// Len returns the current number of cached entries (including potentially
// expired ones that haven't been evicted yet).
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}
