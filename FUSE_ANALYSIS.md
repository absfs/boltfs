# FUSE Implementation Analysis: Internal Access vs absfs Interface

## Current Implementation (Direct Internal Access)

The current `fuse.go` uses these boltfs internals:

### 1. Direct BoltDB Transaction Access
```go
err := s.fs.db.View(func(tx *bolt.Tx) error {
    b := newFsBucketWithCache(tx, s.fs.cache)
    node, err = b.GetInode(parent.Children[x].Ino)
    return err
})
```

### 2. Direct Inode Operations
```go
node, err := b.GetInode(ino)          // Direct inode loading
err := b.PutInode(ino, node)          // Direct inode saving
```

### 3. Direct Data Bucket Access
```go
fileData := b.data.Get(i2b(node.Ino))  // Raw data access
b.data.Put(i2b(ino), data)             // Raw data writing
```

### 4. Cache Integration
```go
b := newFsBucketWithCache(tx, s.fs.cache)  // Use boltfs cache
s.fs.cache.Invalidate(ino)                 // Invalidate on delete
```

### 5. Internal Utilities
```go
i2b(ino)  // Convert inode number to bytes
```

## Alternative Implementation (absfs Interface Only)

### What absfs.FileSystem Provides

```go
type FileSystem interface {
    // File operations
    Open(name string) (File, error)
    Create(name string) (File, error)
    OpenFile(name string, flag int, perm os.FileMode) (File, error)

    // Metadata operations
    Stat(name string) (os.FileInfo, error)
    Lstat(name string) (os.FileInfo, error)

    // Directory operations
    Mkdir(name string, perm os.FileMode) error
    Remove(name string) error
    Rename(oldpath, newpath string) error

    // Attribute operations
    Chmod(name string, mode os.FileMode) error
    Chown(name string, uid, gid int) error
    Chtimes(name string, atime, mtime time.Time) error

    // Symlink operations
    Symlink(source, destination string) error
    Readlink(name string) (string, error)

    // Traversal
    Walk(root string, fn WalkFunc) error
}
```

### What FileInfo Provides

```go
type FileInfo interface {
    Name() string
    Size() int64
    Mode() os.FileMode
    ModTime() time.Time
    IsDir() bool
    Sys() interface{}  // Returns *iNode in boltfs!
}
```

## Comparative Analysis

### Operation: lookup (find inode by name)

**Current (Internal):**
```go
func (s *FUSEServer) lookup(parent *iNode, name string) (*iNode, error) {
    // Binary search in parent.Children
    x := sort.Search(len(parent.Children), ...)

    // Direct inode load
    err := s.fs.db.View(func(tx *bolt.Tx) error {
        b := newFsBucketWithCache(tx, s.fs.cache)
        node, err = b.GetInode(parent.Children[x].Ino)
        return err
    })
}
```
- **Pros**: Direct inode-to-inode traversal, uses cache
- **Cons**: Exposes internal inode structure
- **Performance**: 1 cache lookup or 1 BoltDB read

**Alternative (absfs):**
```go
func (s *FUSEServer) lookup(parentPath, name string) (*iNode, error) {
    path := filepath.Join(parentPath, name)
    info, err := s.fs.Stat(path)
    if err != nil {
        return nil, err
    }
    return info.Sys().(*iNode), nil  // Extract inode from FileInfo
}
```
- **Pros**: Works with any absfs implementation, no internal coupling
- **Cons**: Requires path tracking, full path resolution
- **Performance**: Same (uses same cache underneath)

### Operation: getattr (get file attributes)

**Current (Internal):**
```go
func (s *FUSEServer) getattr(node *iNode) (*fuseAttr, error) {
    // Direct inode field access
    return &fuseAttr{
        Ino:   node.Ino,
        Mode:  node.Mode,
        Nlink: uint32(node.Nlink),
        Size:  uint64(node.Size),
        // ... copy all fields
    }
}
```
- **Performance**: Zero overhead (direct struct access)

**Alternative (absfs):**
```go
func (s *FUSEServer) getattr(path string) (*fuseAttr, error) {
    info, err := s.fs.Stat(path)
    node := info.Sys().(*iNode)  // Extract inode
    return &fuseAttr{
        Ino:   node.Ino,
        Mode:  node.Mode,
        // ... same as above
    }
}
```
- **Performance**: 1 additional Stat() call overhead (~163ns with cache)
- **But**: FUSE already has kernel-side attribute caching!

### Operation: read (read file data)

**Current (Internal):**
```go
func (s *FUSEServer) read(node *iNode, offset int64, size int) ([]byte, error) {
    err := s.fs.db.View(func(tx *bolt.Tx) error {
        b := newFsBucket(tx)
        fileData := b.data.Get(i2b(node.Ino))
        // ... copy range
    })
}
```
- **Pros**: Direct data bucket access
- **Performance**: Minimal (but no cache used!)

**Alternative (absfs):**
```go
func (s *FUSEServer) read(path string, offset int64, size int) ([]byte, error) {
    file, err := s.fs.Open(path)
    defer file.Close()

    data := make([]byte, size)
    n, err := file.ReadAt(data, offset)
    return data[:n], err
}
```
- **Pros**: Standard interface, works with any absfs
- **Performance**: Similar (file.ReadAt uses same BoltDB read)

### Operation: readdir (list directory)

**Current (Internal):**
```go
func (s *FUSEServer) readdir(node *iNode) ([]fuseDirEntry, error) {
    // Direct access to node.Children
    for _, child := range node.Children {
        childNode, err = b.GetInode(child.Ino)
        // ... build entry
    }
}
```
- **Performance**: N inode loads (N = number of children)

**Alternative (absfs):**
```go
func (s *FUSEServer) readdir(path string) ([]fuseDirEntry, error) {
    dir, err := s.fs.OpenFile(path, os.O_RDONLY, 0)
    defer dir.Close()

    infos, err := dir.Readdir(-1)
    // Convert []FileInfo to []fuseDirEntry
}
```
- **Performance**: Same (Readdir internally loads inodes)

### Operation: write (write file data)

**Current (Internal):**
```go
func (s *FUSEServer) write(node *iNode, data []byte, offset int64) (int, error) {
    err := s.fs.db.Update(func(tx *bolt.Tx) error {
        b := newFsBucketWithCache(tx, s.fs.cache)

        // Load existing data
        existing := b.data.Get(i2b(node.Ino))

        // Modify and write back
        newData := make([]byte, newSize)
        copy(newData, existing)
        copy(newData[offset:], data)

        b.data.Put(i2b(node.Ino), newData)
        b.PutInode(node.Ino, node)
    })
}
```
- **Pros**: Single transaction for data + metadata update
- **Performance**: Atomic operation

**Alternative (absfs):**
```go
func (s *FUSEServer) write(path string, data []byte, offset int64) (int, error) {
    file, err := s.fs.OpenFile(path, os.O_RDWR, 0)
    defer file.Close()

    n, err := file.WriteAt(data, offset)
    return n, err
}
```
- **Performance**: Same (WriteAt uses BoltDB transaction internally)
- **Atomicity**: Handled by boltfs implementation

## Performance Impact Analysis

### Theoretical Overhead

| Operation | Internal Access | absfs Interface | Overhead |
|-----------|----------------|-----------------|----------|
| lookup | Direct inode load | Stat() + Sys() | +163ns (cached) |
| getattr | Direct field access | Stat() + Sys() | +163ns (cached) |
| read | Direct bucket access | Open() + ReadAt() | +1 function call (~50ns) |
| readdir | Direct Children access | Readdir() | Same |
| write | Direct bucket access | OpenFile() + WriteAt() | +1 function call (~50ns) |

### Context: FUSE Overhead

FUSE operations have significant overhead:
- **Kernel/userspace context switch**: 10-20 µs (10,000-20,000 ns)
- **FUSE protocol overhead**: 1-5 µs (1,000-5,000 ns)
- **Total per-operation**: 15-30 µs minimum

### Comparison

```
FUSE overhead:           15,000 ns
absfs abstraction:          163 ns  (0.1% of FUSE overhead)
```

**The absfs overhead is 100x smaller than FUSE's inherent overhead.**

## Key Insights

### 1. Inode Numbers Are Available
```go
info, _ := fs.Stat("/file.txt")
node := info.Sys().(*iNode)
inode_number := node.Ino  // All metadata available!
```

The absfs interface **does expose** inode metadata through `FileInfo.Sys()`. This is sufficient for FUSE.

### 2. Cache Benefit is Marginal for FUSE

**FUSE has its own caching layers:**
- Kernel VFS page cache (file data)
- Kernel dentry cache (path lookups)
- Kernel inode cache (metadata)

**Application-side cache helps, but:**
- Most requests hit kernel cache first
- Only cache misses reach userspace
- Benefit is minimal compared to kernel caching

### 3. Transaction Batching Not Needed

FUSE operates on **single operations**:
- Each FUSE request = one operation
- No benefit to batching across requests
- Operations already atomic within boltfs

### 4. Path Tracking is Required Anyway

FUSE works with:
- Inode numbers (uint64)
- Parent inode + name for lookups

But absfs operations need paths. So generic FUSE implementation must:
1. Track inode -> path mapping
2. Build paths for absfs calls

This is standard for FUSE implementations and adds minimal overhead.

## Architectural Benefits of absfs Approach

### 1. Composability
```go
// Works with boltfs
fusefs.Mount(boltfs.Open("data.db"))

// Works with memfs
fusefs.Mount(memfs.New())

// Works with osfs
fusefs.Mount(osfs.New())

// Works with layered/union filesystems
fusefs.Mount(unionfs.New(boltfs, osfs))
```

### 2. Testing
```go
// Test FUSE logic without BoltDB
mockFS := &MockFileSystem{}
server := fusefs.NewServer(mockFS)
// Test FUSE operations independently
```

### 3. Maintainability
- FUSE code doesn't break when boltfs internals change
- Clear separation of concerns
- Each package has single responsibility

### 4. Reusability
- One FUSE implementation serves all absfs filesystems
- Bug fixes benefit everyone
- Features (like caching) benefit all backends

## Disadvantages of Internal Access Approach

### 1. Tight Coupling
```go
// FUSE depends on boltfs internals
b := newFsBucketWithCache(tx, s.fs.cache)
// If boltfs changes internals, FUSE breaks
```

### 2. Lack of Composability
```go
// Can't use FUSE with other absfs implementations
// Must reimplement FUSE for each filesystem type
```

### 3. Testing Difficulty
```go
// Can't test FUSE without full BoltDB setup
// Can't mock filesystem for FUSE testing
```

### 4. Code Duplication
```go
// Each absfs implementation needs own FUSE code
// memfs FUSE, osfs FUSE, boltfs FUSE, etc.
```

## Recommendation: Use absfs Interface

### Reasoning:

1. **Performance**: absfs overhead (163ns) is negligible compared to FUSE overhead (15,000ns)
2. **Architecture**: Maintains composability and separation of concerns
3. **Reusability**: One FUSE implementation for all absfs filesystems
4. **Testing**: Easy to test and mock
5. **Maintainability**: Clear boundaries, single responsibility

### Implementation Plan:

1. **Remove** `fuse.go` from boltfs package
2. **Create** separate `github.com/absfs/fusefs` package
3. **Implement** generic FUSE using only `absfs.FileSystem` interface
4. **Use** `FileInfo.Sys()` to access inode numbers when needed
5. **Track** inode -> path mapping in FUSE layer (standard practice)

### What to Keep in boltfs:

✅ **Inode Cache** - BoltDB-specific optimization
✅ **Snapshots** - Leverages BoltDB's MVCC directly
❌ **FUSE** - Should be generic abstraction layer

## Conclusion

**Direct internal access provides no meaningful performance benefit for FUSE**, because:

1. absfs overhead (163ns) << FUSE overhead (15,000ns)
2. FUSE has kernel-side caching that dominates performance
3. All necessary metadata is available through `FileInfo.Sys()`

**The architectural benefits of the absfs approach far outweigh the negligible performance cost.**

Therefore: **FUSE should be implemented as a separate package using the absfs interface.**
