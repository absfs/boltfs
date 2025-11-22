
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

Store file content in any `absfs.FileSystem` implementation while keeping metadata in BoltDB:

```go
import (
    "github.com/absfs/boltfs"
    "github.com/absfs/memfs"  // or any absfs implementation
)

// Use default BoltDB storage (backward compatible)
fs, _ := boltfs.Open("myfs.db", "")
defer fs.Close()

// Use memfs for file content (in-memory storage)
contentFS, _ := memfs.NewFS()
fs, _ := boltfs.OpenWithContentFS("myfs.db", "", contentFS)
defer fs.Close()

// Or set content filesystem on existing filesystem
contentFS, _ := memfs.NewFS()
fs.SetContentFS(contentFS)

// Use with existing bolt.DB
db, _ := bolt.Open("myfs.db", 0644, nil)
contentFS, _ := memfs.NewFS()
fs, _ := boltfs.NewFSWithContentFS(db, "", contentFS)
```

**Custom Storage Backends:**

Any `absfs.FileSystem` implementation can be used:

```go
// Example: osfs for local filesystem storage
type OSFileSystem struct {
    basePath string
}

func (fs *OSFileSystem) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
    // Implement absfs.FileSystem interface
    // Store files in basePath + name
}
// ... implement other absfs.FileSystem methods

// Use custom filesystem
osfs := &OSFileSystem{basePath: "/data/content"}
fs, _ := boltfs.OpenWithContentFS("myfs.db", "", osfs)
```

**Benefits:**
- **Idiomatic design**: Uses standard `absfs.FileSystem` interface
- **Composable**: Chain/layer multiple absfs implementations
- **Rich ecosystem**: Use any existing absfs implementation (memfs, osfs, s3fs, etc.)
- **Backward compatible**: Defaults to BoltDB storage
- **Automatic cleanup**: Files deleted from external storage when removed

## Coming soon

- Improved test coverage
- Error for error match to `os` package implementations
- FastWalk high performance walker (non sorted, os.FileMode only)
- FUSE mounting support via separate absfs-compatible package

## License

MIT license. See LICENSE file for more information.

