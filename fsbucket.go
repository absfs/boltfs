package boltfs

import (
	"os"
	filepath "path"
	"strings"

	bolt "github.com/etcd-io/bbolt"
)

var buckets = []string{"state", "inodes", "data", "symlinks"}

// 0 is nil for iNode ids
var nilIno uint64

// fsBucket provides a unified interface to a potentally nested bucket in which
// the boltfs buckets can be found.
type fsBucket struct {
	state    *bolt.Bucket
	inodes   *bolt.Bucket
	data     *bolt.Bucket
	symlinks *bolt.Bucket
}

type bucketer interface {
	Bucket([]byte) *bolt.Bucket
	CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
}

// newFsBucket creats a new fsBucket
func newFsBucket(b bucketer) *fsBucket {

	state := b.Bucket([]byte("state"))
	if state == nil {
		return nil
	}
	inodes := b.Bucket([]byte("inodes"))
	if inodes == nil {
		return nil
	}
	data := b.Bucket([]byte("data"))
	if data == nil {
		return nil
	}
	symlinks := b.Bucket([]byte("symlinks"))
	if symlinks == nil {
		return nil
	}
	return &fsBucket{
		state:    state,
		inodes:   inodes,
		data:     data,
		symlinks: symlinks,
	}

}

// NextInode returns the next iNode id (Ino), or an error.
func (f *fsBucket) NextInode() (uint64, error) {
	return f.inodes.NextSequence()
}

// InodeInit initializes the `inodes` bucket if necessary. This is done
// because iNode 0 represents the `nil` iNode. An empty iNode is written to
// the `inodes` bucket to cause boltdb to increment the `NextSequence` counter
// to 1.
func (f *fsBucket) InodeInit() error {
	node := newInode(os.ModeDir | 0755)
	node.countUp() // iNode garbage collector should not remove nil, or root
	for count := 2 - f.inodes.Stats().KeyN; count > 0; count-- {
		node.Ino = uint64(2 - count)
		err := f.PutInode(node.Ino, node)
		if err != nil {
			return err
		}
	}
	return nil
}

// bucketInit initializes the necessary buckets for boltfs in the possibly nested
// path defined by `bucketpath`. `bucketpath` can be an empty string, or a slash
// delimited path of nested buckets within which to create the boltfs buckets.
func bucketInit(tx *bolt.Tx, bucketpath string) error {
	// create top level buckets
	/*
		if bucketpath == "" {
			for _, name := range buckets {
				_, err := tx.CreateBucketIfNotExists([]byte(name))
				if err != nil {
					return err
				}
			}
			return nil
		}
	*/

	var err error
	var b bucketer
	b = tx

	// create nested buckets
	names := strings.Split(strings.Trim(filepath.Clean(bucketpath), "/"), "/")

	var paths []string
	for _, name := range names {
		if name == "" || name == "." {
			continue
		}
		paths = append(paths, name)
		b, err = b.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
	}

	for _, name := range buckets {
		_, err := b.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
	}

	return nil
}

// openBucket opens a possibly nested bucket defined as a slash delimited path
// in bucketpath.
func openBucket(tx *bolt.Tx, bucketpath string) (bucketer, error) {

	var b bucketer
	b = tx

	names := strings.Split(strings.Trim(filepath.Clean(bucketpath), "/"), "/")
	for _, name := range names {
		if name == "" || name == "." {
			continue
		}
		b = b.Bucket([]byte(name))
		if b == nil {
			return nil, os.ErrNotExist
		}
	}
	return b, nil

}

// LoadOrSet loads the value stored in `key` from the `state` bucket, or sets
// it to `value` if `key` is empty.
func (f *fsBucket) LoadOrSet(key string, value []byte) ([]byte, error) {
	data := f.state.Get([]byte(key))
	if data != nil {
		return data, nil
	}

	err := f.state.Put([]byte(key), value)
	return value, err
}

// PutInode stores an iNode to the given `ino`. If `ino` is `nilIno` then the
// node is saved as the next ino. `node.Ino` will be set to the ino underwhich
// the node is saved, wether that be the `ino` argument or a newly generated
// id.
func (f *fsBucket) PutInode(ino uint64, node *iNode) error {
	var err error
	if ino == 0 {
		ino, err = f.NextInode()
		if err != nil {
			return err
		}
	}
	node.Ino = ino
	return encodeNode(f.inodes, ino, node)
}

// GetInode returns the stored iNode for the given `ino` id, or an error.
func (f *fsBucket) GetInode(ino uint64) (*iNode, error) {
	if ino == nilIno {
		return nil, errInvalidIno
	}

	node := newInode(0)
	err := decodeNode(f.inodes, ino, node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// Get returns the state value for the given` key`.
func (f *fsBucket) Get(key string) ([]byte, error) {
	data := f.state.Get([]byte(key))
	if data == nil {
		return nil, os.ErrNotExist
	}
	return data, nil
}

// Put sets the state value for the given` key`.
func (f *fsBucket) Put(key string, data []byte) error {
	return f.state.Put([]byte(key), data)
}

// Symlink creates or replaces the relative or absolute path associated with
// `ino`. The node identified by `ino` is not evaluated so care must be taken
// to only create symlinks to iNodes that already exist and have a Mode set to
// os.ModeSymlink.
//
// To remove a symlink set the path to an empty string"".
func (f *fsBucket) Symlink(ino uint64, path string) error {
	if ino == 0 {
		return errNilNode
	}
	if path == "" {
		return f.symlinks.Delete(i2b(ino))
	}
	return f.symlinks.Put(i2b(ino), []byte(path))
}

// Readlink reads the symbolic link string associated with `ino`. or an empty
// string.
func (f *fsBucket) Readlink(ino uint64) string {
	if ino == 0 {
		return ""
	}

	data := f.symlinks.Get(i2b(ino))
	if data == nil {
		return ""
	}

	return string(data)
}
