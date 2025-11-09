// +build linux darwin freebsd

package boltfs

import (
	"context"
	"fmt"
	"os"
	filepath "path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

// FUSEServer provides FUSE (Filesystem in Userspace) support for boltfs.
// This allows mounting boltfs as a real filesystem that can be accessed
// by any application.
//
// Note: This is a basic implementation that provides the interface.
// A complete FUSE implementation would require importing a FUSE library
// like bazil.org/fuse or github.com/hanwen/go-fuse.
type FUSEServer struct {
	fs         *FileSystem
	mountPoint string
	mounted    bool
	mu         sync.Mutex

	// File handle tracking
	nextHandle uint64
	handles    map[uint64]*fuseHandle
	handlesMu  sync.RWMutex
}

// fuseHandle represents an open file handle in FUSE.
type fuseHandle struct {
	node   *iNode
	path   string
	flags  int
	offset int64
}

// NewFUSEServer creates a new FUSE server for the filesystem.
func (fs *FileSystem) NewFUSEServer(mountPoint string) *FUSEServer {
	return &FUSEServer{
		fs:         fs,
		mountPoint: mountPoint,
		handles:    make(map[uint64]*fuseHandle),
		nextHandle: 1,
	}
}

// Mount mounts the filesystem at the specified mount point.
// This is a placeholder implementation. A real implementation would:
// 1. Import a FUSE library (bazil.org/fuse or github.com/hanwen/go-fuse)
// 2. Initialize the FUSE connection
// 3. Register filesystem operation handlers
// 4. Start serving FUSE requests
func (s *FUSEServer) Mount() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.mounted {
		return fmt.Errorf("already mounted at %s", s.mountPoint)
	}

	// Check if mount point exists and is a directory
	info, err := os.Stat(s.mountPoint)
	if err != nil {
		return fmt.Errorf("mount point error: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("mount point must be a directory")
	}

	// In a real implementation, you would:
	// 1. Call fuse.Mount(s.mountPoint, ...)
	// 2. Start serving FUSE requests
	// 3. Register all the FUSE operation handlers below

	s.mounted = true
	return fmt.Errorf("FUSE support requires a FUSE library (bazil.org/fuse or github.com/hanwen/go-fuse). This is a framework implementation")
}

// Unmount unmounts the filesystem.
func (s *FUSEServer) Unmount() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.mounted {
		return fmt.Errorf("not mounted")
	}

	// In a real implementation, you would:
	// 1. Call fuse.Unmount(s.mountPoint)
	// 2. Wait for the server to stop

	s.mounted = false
	return nil
}

// IsMounted returns true if the filesystem is currently mounted.
func (s *FUSEServer) IsMounted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mounted
}

// The following methods implement the FUSE filesystem operations.
// In a real implementation, these would be registered with the FUSE library.

// lookup looks up a directory entry by name and returns its inode.
func (s *FUSEServer) lookup(ctx context.Context, parent *iNode, name string) (*iNode, error) {
	if !parent.IsDir() {
		return nil, syscall.ENOTDIR
	}

	// Binary search for the child
	x := sort.Search(len(parent.Children), func(i int) bool {
		return parent.Children[i].Name >= name
	})

	if x == len(parent.Children) || parent.Children[x].Name != name {
		return nil, syscall.ENOENT
	}

	// Load the child inode
	var node *iNode
	err := s.fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}
		var err error
		node, err = b.GetInode(parent.Children[x].Ino)
		return err
	})

	if err != nil {
		return nil, syscall.EIO
	}

	return node, nil
}

// getattr returns file attributes for the given inode.
func (s *FUSEServer) getattr(ctx context.Context, node *iNode) (*fuseAttr, error) {
	attr := &fuseAttr{
		Ino:   node.Ino,
		Mode:  node.Mode,
		Nlink: uint32(node.Nlink),
		Size:  uint64(node.Size),
		Atime: node.Atime,
		Mtime: node.Mtime,
		Ctime: node.Ctime,
		Uid:   node.Uid,
		Gid:   node.Gid,
	}

	// Set block size and block count
	attr.Blksize = 4096
	attr.Blocks = (attr.Size + 511) / 512

	return attr, nil
}

// fuseAttr represents FUSE file attributes.
type fuseAttr struct {
	Ino     uint64
	Mode    os.FileMode
	Nlink   uint32
	Size    uint64
	Blocks  uint64
	Blksize uint32
	Atime   time.Time
	Mtime   time.Time
	Ctime   time.Time
	Uid     uint32
	Gid     uint32
}

// readdir reads directory entries.
func (s *FUSEServer) readdir(ctx context.Context, node *iNode) ([]fuseDirEntry, error) {
	if !node.IsDir() {
		return nil, syscall.ENOTDIR
	}

	entries := make([]fuseDirEntry, 0, len(node.Children))

	// Add . and ..
	entries = append(entries, fuseDirEntry{
		Ino:  node.Ino,
		Name: ".",
		Type: os.ModeDir,
	})

	// For .., we'd need to track parent inodes
	entries = append(entries, fuseDirEntry{
		Ino:  node.Ino, // Simplified: use same inode
		Name: "..",
		Type: os.ModeDir,
	})

	// Add children
	for _, child := range node.Children {
		var childNode *iNode
		err := s.fs.db.View(func(tx *bolt.Tx) error {
			b := newFsBucketWithCache(tx, s.fs.cache)
			if b == nil {
				return os.ErrNotExist
			}
			var err error
			childNode, err = b.GetInode(child.Ino)
			return err
		})

		if err != nil {
			continue // Skip entries we can't read
		}

		entries = append(entries, fuseDirEntry{
			Ino:  child.Ino,
			Name: child.Name,
			Type: childNode.Mode.Type(),
		})
	}

	return entries, nil
}

// fuseDirEntry represents a FUSE directory entry.
type fuseDirEntry struct {
	Ino  uint64
	Name string
	Type os.FileMode
}

// read reads data from a file.
func (s *FUSEServer) read(ctx context.Context, node *iNode, offset int64, size int) ([]byte, error) {
	if node.IsDir() {
		return nil, syscall.EISDIR
	}

	var data []byte
	err := s.fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		if b == nil {
			return os.ErrNotExist
		}

		fileData := b.data.Get(i2b(node.Ino))
		if fileData == nil {
			// Empty file
			data = []byte{}
			return nil
		}

		// Calculate read range
		if offset >= int64(len(fileData)) {
			data = []byte{}
			return nil
		}

		end := offset + int64(size)
		if end > int64(len(fileData)) {
			end = int64(len(fileData))
		}

		// Return a copy
		data = make([]byte, end-offset)
		copy(data, fileData[offset:end])
		return nil
	})

	if err != nil {
		return nil, syscall.EIO
	}

	return data, nil
}

// write writes data to a file.
func (s *FUSEServer) write(ctx context.Context, node *iNode, data []byte, offset int64) (int, error) {
	if node.IsDir() {
		return 0, syscall.EISDIR
	}

	var written int
	err := s.fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}

		// Load existing data
		existing := b.data.Get(i2b(node.Ino))

		// Calculate new size
		newSize := offset + int64(len(data))
		if newSize > int64(len(existing)) {
			// Expand the file
			newData := make([]byte, newSize)
			copy(newData, existing)
			copy(newData[offset:], data)

			err := b.data.Put(i2b(node.Ino), newData)
			if err != nil {
				return err
			}

			node.Size = newSize
		} else {
			// Overwrite existing data
			newData := make([]byte, len(existing))
			copy(newData, existing)
			copy(newData[offset:], data)

			err := b.data.Put(i2b(node.Ino), newData)
			if err != nil {
				return err
			}
		}

		node.modified()
		err := b.PutInode(node.Ino, node)
		if err != nil {
			return err
		}

		written = len(data)
		return nil
	})

	if err != nil {
		return 0, syscall.EIO
	}

	return written, nil
}

// create creates a new file.
func (s *FUSEServer) create(ctx context.Context, parent *iNode, name string, mode os.FileMode) (*iNode, error) {
	if !parent.IsDir() {
		return nil, syscall.ENOTDIR
	}

	var newNode *iNode
	err := s.fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}

		// Check if file already exists
		x := sort.Search(len(parent.Children), func(i int) bool {
			return parent.Children[i].Name >= name
		})
		if x < len(parent.Children) && parent.Children[x].Name == name {
			return os.ErrExist
		}

		// Create new inode
		newNode = newInode(mode)
		newNode.countUp()
		err := b.PutInode(0, newNode) // 0 = allocate new inode number
		if err != nil {
			return err
		}

		// Link to parent
		_, err = parent.Link(name, newNode.Ino)
		if err != nil {
			return err
		}

		// Save parent
		return b.PutInode(parent.Ino, parent)
	})

	if err != nil {
		return nil, syscall.EIO
	}

	return newNode, nil
}

// mkdir creates a new directory.
func (s *FUSEServer) mkdir(ctx context.Context, parent *iNode, name string, mode os.FileMode) (*iNode, error) {
	return s.create(ctx, parent, name, os.ModeDir|mode)
}

// unlink removes a file from a directory.
func (s *FUSEServer) unlink(ctx context.Context, parent *iNode, name string) error {
	if !parent.IsDir() {
		return syscall.ENOTDIR
	}

	err := s.fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}

		// Unlink from parent
		ino, err := parent.Unlink(name)
		if err != nil {
			return err
		}

		// Save parent
		err = b.PutInode(parent.Ino, parent)
		if err != nil {
			return err
		}

		// Load the node to decrement link count
		node, err := b.GetInode(ino)
		if err != nil {
			return err
		}

		node.countDown()

		// If no more links, delete the inode and data
		if node.Nlink == 0 {
			b.data.Delete(i2b(ino))
			b.inodes.Delete(i2b(ino))
			b.symlinks.Delete(i2b(ino))
			if s.fs.cache != nil {
				s.fs.cache.Invalidate(ino)
			}
		} else {
			err = b.PutInode(ino, node)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return syscall.EIO
	}

	return nil
}

// rename renames a file or directory.
func (s *FUSEServer) rename(ctx context.Context, oldParent *iNode, oldName string, newParent *iNode, newName string) error {
	err := s.fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}

		// Unlink from old parent
		ino, err := oldParent.Unlink(oldName)
		if err != nil {
			return err
		}

		// Link to new parent (this may replace an existing entry)
		oldIno, err := newParent.Link(newName, ino)
		if err != nil {
			// Roll back
			oldParent.Link(oldName, ino)
			return err
		}

		// If we replaced an existing entry, handle it
		if oldIno != 0 {
			node, err := b.GetInode(oldIno)
			if err == nil {
				node.countDown()
				if node.Nlink == 0 {
					b.data.Delete(i2b(oldIno))
					b.inodes.Delete(i2b(oldIno))
					b.symlinks.Delete(i2b(oldIno))
					if s.fs.cache != nil {
						s.fs.cache.Invalidate(oldIno)
					}
				} else {
					b.PutInode(oldIno, node)
				}
			}
		}

		// Save both parents
		err = b.PutInode(oldParent.Ino, oldParent)
		if err != nil {
			return err
		}

		if oldParent.Ino != newParent.Ino {
			err = b.PutInode(newParent.Ino, newParent)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return syscall.EIO
	}

	return nil
}

// Helper methods for handle management

func (s *FUSEServer) allocHandle(node *iNode, path string, flags int) uint64 {
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()

	handle := s.nextHandle
	s.nextHandle++

	s.handles[handle] = &fuseHandle{
		node:   copyInode(node),
		path:   path,
		flags:  flags,
		offset: 0,
	}

	return handle
}

func (s *FUSEServer) getHandle(handle uint64) (*fuseHandle, bool) {
	s.handlesMu.RLock()
	defer s.handlesMu.RUnlock()

	h, ok := s.handles[handle]
	return h, ok
}

func (s *FUSEServer) releaseHandle(handle uint64) {
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()

	delete(s.handles, handle)
}

// resolvePath resolves a path to an inode for FUSE operations.
func (s *FUSEServer) resolvePath(path string) (*iNode, error) {
	node := new(iNode)

	err := s.fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucketWithCache(tx, s.fs.cache)
		if b == nil {
			return os.ErrNotExist
		}

		ino := s.fs.rootIno
		loadedNode, err := b.GetInode(ino)
		if err != nil {
			return err
		}
		*node = *loadedNode

		if path == "/" || path == "" {
			return nil
		}

		for _, name := range strings.Split(strings.TrimLeft(filepath.Clean(path), "/"), "/") {
			if name == "" {
				continue
			}

			// find the child's ino or error
			x := sort.Search(len(node.Children), func(i int) bool {
				return node.Children[i].Name >= name
			})
			if x == len(node.Children) || node.Children[x].Name != name {
				return os.ErrNotExist
			}

			// replace node with child or error
			loadedNode, err = b.GetInode(node.Children[x].Ino)
			if err != nil {
				return err
			}
			*node = *loadedNode
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return node, nil
}
