// Package boltfs provides a complete file system implementation for boltdb. This
// implementation includes support for symbolic links and includes a file system
// walker that works just like `filepath.Walk`.
//
// BoltFS now includes advanced features:
//   - Thread-safe in-memory inode cache for improved performance
//   - Snapshot support using BoltDB's MVCC for point-in-time views
//   - FUSE interface framework for mounting as a real filesystem
//
// The inode cache significantly improves read performance by caching frequently
// accessed inodes in memory with configurable size and LRU eviction.
//
// Snapshots provide read-only, consistent views of the filesystem at a point in
// time without blocking writes, implemented using BoltDB's read-only transactions.
//
// The FUSE server provides a framework for implementing full filesystem mounting
// capabilities, requiring integration with a FUSE library like bazil.org/fuse.
package boltfs
