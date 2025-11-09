package boltfs

import (
	"sync"
	"time"
)

// inodeCache provides a thread-safe in-memory cache for inodes to improve
// performance by reducing BoltDB read operations. The cache uses a simple
// LRU-like eviction policy based on access time and size limits.
type inodeCache struct {
	mu      sync.RWMutex
	entries map[uint64]*cacheEntry
	maxSize int
	hits    uint64
	misses  uint64
	enabled bool
}

// cacheEntry represents a cached inode with metadata for cache management.
type cacheEntry struct {
	node     *iNode
	lastUsed time.Time
	dirty    bool // true if the node has been modified but not persisted
}

// newInodeCache creates a new inode cache with the specified maximum size.
// A maxSize of 0 or negative disables the cache.
func newInodeCache(maxSize int) *inodeCache {
	return &inodeCache{
		entries: make(map[uint64]*cacheEntry),
		maxSize: maxSize,
		enabled: maxSize > 0,
	}
}

// Get retrieves an inode from the cache if it exists.
// Returns nil if the inode is not in the cache.
func (c *inodeCache) Get(ino uint64) *iNode {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	entry, ok := c.entries[ino]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil
	}

	// Update access time and hit counter
	c.mu.Lock()
	entry.lastUsed = time.Now()
	c.hits++
	c.mu.Unlock()

	// Return a copy to prevent external modifications
	return copyInode(entry.node)
}

// Put adds or updates an inode in the cache.
func (c *inodeCache) Put(ino uint64, node *iNode, dirty bool) {
	if !c.enabled || node == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict old entries if cache is full
	if len(c.entries) >= c.maxSize {
		c.evictOldest()
	}

	// Store a copy to prevent external modifications
	c.entries[ino] = &cacheEntry{
		node:     copyInode(node),
		lastUsed: time.Now(),
		dirty:    dirty,
	}
}

// Invalidate removes an inode from the cache.
func (c *inodeCache) Invalidate(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	delete(c.entries, ino)
	c.mu.Unlock()
}

// MarkDirty marks a cached inode as dirty (modified but not persisted).
func (c *inodeCache) MarkDirty(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[ino]; ok {
		entry.dirty = true
		entry.lastUsed = time.Now()
	}
}

// MarkClean marks a cached inode as clean (persisted to disk).
func (c *inodeCache) MarkClean(ino uint64) {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.entries[ino]; ok {
		entry.dirty = false
	}
}

// IsDirty checks if a cached inode has unsaved modifications.
func (c *inodeCache) IsDirty(ino uint64) bool {
	if !c.enabled {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if entry, ok := c.entries[ino]; ok {
		return entry.dirty
	}
	return false
}

// GetDirty returns all dirty (unsaved) inodes in the cache.
func (c *inodeCache) GetDirty() map[uint64]*iNode {
	if !c.enabled {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	dirty := make(map[uint64]*iNode)
	for ino, entry := range c.entries {
		if entry.dirty {
			dirty[ino] = copyInode(entry.node)
		}
	}
	return dirty
}

// Flush removes all entries from the cache.
func (c *inodeCache) Flush() {
	if !c.enabled {
		return
	}

	c.mu.Lock()
	c.entries = make(map[uint64]*cacheEntry)
	c.mu.Unlock()
}

// Stats returns cache statistics.
func (c *inodeCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return CacheStats{
		Size:    len(c.entries),
		MaxSize: c.maxSize,
		Hits:    c.hits,
		Misses:  c.misses,
		Enabled: c.enabled,
	}
}

// CacheStats contains statistics about cache performance.
type CacheStats struct {
	Size    int    // Current number of cached entries
	MaxSize int    // Maximum cache size
	Hits    uint64 // Number of cache hits
	Misses  uint64 // Number of cache misses
	Enabled bool   // Whether the cache is enabled
}

// HitRate returns the cache hit rate as a percentage.
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total) * 100
}

// evictOldest removes the least recently used entry from the cache.
// Must be called with c.mu locked.
func (c *inodeCache) evictOldest() {
	var oldestIno uint64
	var oldestTime time.Time
	first := true

	for ino, entry := range c.entries {
		// Don't evict dirty entries
		if entry.dirty {
			continue
		}

		if first || entry.lastUsed.Before(oldestTime) {
			oldestIno = ino
			oldestTime = entry.lastUsed
			first = false
		}
	}

	if !first {
		delete(c.entries, oldestIno)
	}
}

// Enable enables the cache with the specified maximum size.
func (c *inodeCache) Enable(maxSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxSize = maxSize
	c.enabled = maxSize > 0
	if !c.enabled {
		c.entries = make(map[uint64]*cacheEntry)
	}
}

// Disable disables the cache and flushes all entries.
func (c *inodeCache) Disable() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enabled = false
	c.entries = make(map[uint64]*cacheEntry)
}
