
# BoltFS - A Complete Filesystem Implementation for BoltDB
BoltFS provides a full featured filesystem on top of a boltdb database. This
package implements most of the filesystem functions from the `os` standard
library package, even including support for symbolic links.

## Features

- Compatible with the abstract filesystem interface `absfs.SymlinkFileSystem`
- Support for hard and soft linking
- Walk method like `filepath.Walk`
- **Thread-safe in-memory inode cache** for improved performance
- **Snapshot support** using BoltDB's MVCC for point-in-time filesystem views
- **External content storage** for storing file content outside BoltDB (filesystem, S3, etc.)
- Extensive tests with >90% coverage

### Inode Cache

The inode cache provides significant performance improvements by caching frequently accessed inodes in memory:

```go
fs, _ := boltfs.Open("myfs.db", "")
defer fs.Close()

// Get cache statistics
stats := fs.CacheStats()
fmt.Printf("Hit rate: %.2f%%\n", stats.HitRate())

// Adjust cache size (default: 1000)
fs.SetCacheSize(5000)

// Flush cache if needed
fs.FlushCache()
```

### Snapshots

Create read-only point-in-time views of the filesystem:

```go
// Create a snapshot
snapshot, _ := fs.CreateSnapshot("backup-2024")
defer snapshot.Release()

// Read files from snapshot
data, _ := snapshot.ReadFile("/config.json")

// Walk snapshot filesystem
snapshot.Walk("/", func(path string, info os.FileInfo, err error) error {
    fmt.Println(path)
    return nil
})

// Restore files from snapshot
snapshot.CopyToFS("/config.json", "/config-restored.json")

// Use snapshot manager for multiple snapshots
sm := fs.NewSnapshotManager()
sm.Create("daily-backup")
snap, _ := sm.Get("daily-backup")
```

### External Content Storage

Store file content in external storage systems while keeping metadata in BoltDB:

```go
// Use default BoltDB storage (backward compatible)
fs, _ := boltfs.Open("myfs.db", "")
defer fs.Close()

// Use filesystem storage for file content
contentStore, _ := boltfs.NewFilesystemContentStore("/path/to/content")
fs, _ := boltfs.OpenWithContentStore("myfs.db", "", contentStore)
defer fs.Close()

// Or set content store on existing filesystem
contentStore, _ := boltfs.NewFilesystemContentStore("/path/to/content")
fs.SetContentStore(contentStore)

// Implement custom storage backend
type S3ContentStore struct {
    bucket string
    // ... S3 client fields
}

func (s *S3ContentStore) Put(ino uint64, data []byte) error {
    // Upload to S3
    return nil
}

func (s *S3ContentStore) Get(ino uint64) ([]byte, error) {
    // Download from S3
    return nil, nil
}

func (s *S3ContentStore) Delete(ino uint64) error {
    // Delete from S3
    return nil
}

func (s *S3ContentStore) Exists(ino uint64) bool {
    // Check if exists in S3
    return false
}

func (s *S3ContentStore) Close() error {
    // Cleanup
    return nil
}

// Use custom storage
s3Store := &S3ContentStore{bucket: "my-bucket"}
fs, _ := boltfs.NewFSWithContentStore(db, "", s3Store)
```

**Benefits:**
- Separate metadata (fast BoltDB) from content (scalable storage)
- Support for any storage backend (filesystem, S3, Azure, etc.)
- Backward compatible - defaults to BoltDB storage
- Proper cleanup when files are deleted

## Coming soon

- Improved test coverage
- Error for error match to `os` package implementations
- FastWalk high performance walker (non sorted, os.FileMode only)
- FUSE mounting support via separate absfs-compatible package

## License

MIT license. See LICENSE file for more information.

