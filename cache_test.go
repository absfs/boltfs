package boltfs

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// tempFile returns a temporary file path for testing
func tempFile() string {
	return fmt.Sprintf("test_boltfs_%d.db", time.Now().UnixNano())
}

func TestInodeCache_Basic(t *testing.T) {
	cache := newInodeCache(10)

	if !cache.enabled {
		t.Error("cache should be enabled with positive size")
	}

	// Test empty cache
	if node := cache.Get(1); node != nil {
		t.Error("expected nil for cache miss")
	}

	// Test put and get
	testNode := newInode(0644)
	testNode.Ino = 1
	cache.Put(1, testNode, false)

	retrieved := cache.Get(1)
	if retrieved == nil {
		t.Fatal("expected node to be in cache")
	}
	if retrieved.Ino != 1 {
		t.Errorf("expected ino 1, got %d", retrieved.Ino)
	}

	// Test that returned node is a copy
	retrieved.Mode = 0777
	retrieved2 := cache.Get(1)
	if retrieved2.Mode == 0777 {
		t.Error("cache should return copies, not references")
	}
}

func TestInodeCache_Invalidate(t *testing.T) {
	cache := newInodeCache(10)

	testNode := newInode(0644)
	testNode.Ino = 1
	cache.Put(1, testNode, false)

	if cache.Get(1) == nil {
		t.Fatal("node should be in cache")
	}

	cache.Invalidate(1)

	if node := cache.Get(1); node != nil {
		t.Error("node should have been invalidated")
	}
}

func TestInodeCache_DirtyTracking(t *testing.T) {
	cache := newInodeCache(10)

	testNode := newInode(0644)
	testNode.Ino = 1

	// Put clean node
	cache.Put(1, testNode, false)
	if cache.IsDirty(1) {
		t.Error("node should not be dirty")
	}

	// Mark dirty
	cache.MarkDirty(1)
	if !cache.IsDirty(1) {
		t.Error("node should be dirty")
	}

	// Mark clean
	cache.MarkClean(1)
	if cache.IsDirty(1) {
		t.Error("node should be clean")
	}

	// Put dirty node
	cache.Put(2, testNode, true)
	if !cache.IsDirty(2) {
		t.Error("node should be dirty")
	}
}

func TestInodeCache_GetDirty(t *testing.T) {
	cache := newInodeCache(10)

	// Add some nodes
	for i := uint64(1); i <= 5; i++ {
		node := newInode(0644)
		node.Ino = i
		dirty := i%2 == 0 // Even inodes are dirty
		cache.Put(i, node, dirty)
	}

	dirtyNodes := cache.GetDirty()
	if len(dirtyNodes) != 2 {
		t.Errorf("expected 2 dirty nodes, got %d", len(dirtyNodes))
	}

	for ino := range dirtyNodes {
		if ino%2 != 0 {
			t.Errorf("expected only even inodes to be dirty, got %d", ino)
		}
	}
}

func TestInodeCache_Eviction(t *testing.T) {
	cache := newInodeCache(3)

	// Fill cache
	for i := uint64(1); i <= 3; i++ {
		node := newInode(0644)
		node.Ino = i
		cache.Put(i, node, false)
		time.Sleep(time.Millisecond) // Ensure different access times
	}

	stats := cache.Stats()
	if stats.Size != 3 {
		t.Errorf("expected cache size 3, got %d", stats.Size)
	}

	// Access node 1 to make it recently used
	cache.Get(1)

	// Add a new node, should evict least recently used (node 2)
	node4 := newInode(0644)
	node4.Ino = 4
	cache.Put(4, node4, false)

	if cache.Get(2) != nil {
		t.Error("node 2 should have been evicted")
	}
	if cache.Get(1) == nil {
		t.Error("node 1 should still be in cache")
	}
	if cache.Get(4) == nil {
		t.Error("node 4 should be in cache")
	}
}

func TestInodeCache_NoDirtyEviction(t *testing.T) {
	cache := newInodeCache(2)

	// Add dirty node
	node1 := newInode(0644)
	node1.Ino = 1
	cache.Put(1, node1, true)

	// Add clean node
	node2 := newInode(0644)
	node2.Ino = 2
	cache.Put(2, node2, false)

	time.Sleep(time.Millisecond)

	// Try to add another node - should evict node 2, not dirty node 1
	node3 := newInode(0644)
	node3.Ino = 3
	cache.Put(3, node3, false)

	if cache.Get(1) == nil {
		t.Error("dirty node 1 should not have been evicted")
	}
	if cache.Get(2) != nil {
		t.Error("clean node 2 should have been evicted")
	}
}

func TestInodeCache_Stats(t *testing.T) {
	cache := newInodeCache(10)

	stats := cache.Stats()
	if stats.Size != 0 {
		t.Errorf("expected size 0, got %d", stats.Size)
	}
	if stats.Hits != 0 || stats.Misses != 0 {
		t.Error("expected zero hits and misses")
	}

	testNode := newInode(0644)
	testNode.Ino = 1
	cache.Put(1, testNode, false)

	// Miss
	cache.Get(2)
	// Hit
	cache.Get(1)

	stats = cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
	if stats.HitRate() != 50.0 {
		t.Errorf("expected 50%% hit rate, got %.2f%%", stats.HitRate())
	}
}

func TestInodeCache_Flush(t *testing.T) {
	cache := newInodeCache(10)

	for i := uint64(1); i <= 5; i++ {
		node := newInode(0644)
		node.Ino = i
		cache.Put(i, node, false)
	}

	if cache.Stats().Size != 5 {
		t.Fatal("cache should have 5 entries")
	}

	cache.Flush()

	if cache.Stats().Size != 0 {
		t.Error("cache should be empty after flush")
	}
}

func TestInodeCache_DisableEnable(t *testing.T) {
	cache := newInodeCache(10)

	testNode := newInode(0644)
	testNode.Ino = 1
	cache.Put(1, testNode, false)

	if cache.Get(1) == nil {
		t.Fatal("node should be in cache")
	}

	// Disable cache
	cache.Disable()

	if cache.enabled {
		t.Error("cache should be disabled")
	}
	if cache.Get(1) != nil {
		t.Error("cache should be flushed when disabled")
	}

	// Try to add when disabled
	cache.Put(2, testNode, false)
	if cache.Get(2) != nil {
		t.Error("cache should not accept entries when disabled")
	}

	// Re-enable
	cache.Enable(10)

	if !cache.enabled {
		t.Error("cache should be enabled")
	}

	cache.Put(3, testNode, false)
	if cache.Get(3) == nil {
		t.Error("cache should accept entries when enabled")
	}
}

func TestInodeCache_ZeroSize(t *testing.T) {
	cache := newInodeCache(0)

	if cache.enabled {
		t.Error("cache should be disabled with size 0")
	}

	testNode := newInode(0644)
	testNode.Ino = 1
	cache.Put(1, testNode, false)

	if cache.Get(1) != nil {
		t.Error("disabled cache should not store entries")
	}
}

func TestFileSystemCache_Integration(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Check that cache is initialized
	stats := fs.CacheStats()
	if !stats.Enabled {
		t.Error("cache should be enabled by default")
	}

	// Create a file
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	// First stat should be a cache miss
	_, err = fs.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Second stat should be a cache hit
	_, err = fs.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	stats = fs.CacheStats()
	if stats.Hits == 0 {
		t.Error("expected at least one cache hit")
	}

	// Test flush
	fs.FlushCache()
	stats = fs.CacheStats()
	if stats.Size != 0 {
		t.Error("cache should be empty after flush")
	}

	// Test cache size change
	fs.SetCacheSize(100)
	stats = fs.CacheStats()
	if stats.MaxSize != 100 {
		t.Errorf("expected max size 100, got %d", stats.MaxSize)
	}

	// Disable cache
	fs.SetCacheSize(0)
	stats = fs.CacheStats()
	if stats.Enabled {
		t.Error("cache should be disabled")
	}
}

func TestInodeCache_Concurrent(t *testing.T) {
	cache := newInodeCache(100)
	done := make(chan bool)

	// Start multiple goroutines accessing the cache
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				ino := uint64(j%10 + 1)
				node := newInode(0644)
				node.Ino = ino

				cache.Put(ino, node, false)
				cache.Get(ino)
				cache.MarkDirty(ino)
				cache.MarkClean(ino)
				cache.IsDirty(ino)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Cache should still be functional
	stats := cache.Stats()
	if stats.Size == 0 {
		t.Error("cache should have entries")
	}
}
