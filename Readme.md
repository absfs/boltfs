
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
- **FUSE interface framework** for mounting as a real filesystem (requires FUSE library)
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

### FUSE Support

Framework for mounting boltfs as a real filesystem:

```go
fuse := fs.NewFUSEServer("/mnt/boltfs")

// Note: Requires a FUSE library like:
// - bazil.org/fuse
// - github.com/hanwen/go-fuse
```

## Coming soon

- Improved test coverage
- Error for error match to `os` package implementations
- FastWalk high performance walker (non sorted, os.FileMode only)
- Support for storing file content externally
- Complete FUSE implementation with library integration

## License

MIT license. See LICENSE file for more information.

