package boltfs

import (
	"os"
	filepath "path"
	"strings"

	bolt "github.com/coreos/bbolt"
)

// file system buckets
var buckets = []string{"state", "inodes", "data", "symlinks"}

// 0 is nil for iNode ids
var nilIno uint64

// fsTx (aka filesystem transaction) provides a unified interface to a and
// bolt.Tx and bucket handles to the potentially nested file system buckets.
type fsTx struct {
	tx *bolt.Tx

	state    *bolt.Bucket
	inodes   *bolt.Bucket
	data     *bolt.Bucket
	symlinks *bolt.Bucket
}

// newFsTx creats a new fsTx
func newFsTx(b bucketer, tx *bolt.Tx) *fsTx {

	var list []*bolt.Bucket
	for i, name := range buckets {
		list = append(list, b.Bucket([]byte(name)))

		if list[i] == nil {
			return nil
		}
	}

	return &fsTx{
		tx:       tx,
		state:    list[0],
		inodes:   list[1],
		data:     list[2],
		symlinks: list[3],
	}

}

type bucketer interface {
	Bucket([]byte) *bolt.Bucket
	CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
}

// update initiates a filesystem modification transaction
func update(db *bolt.DB, bucketpath string) (*fsTx, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	b, err := openBucket(tx, bucketpath)
	if err != nil {
		return nil, err
	}
	return newFsTx(b, tx), nil

}

// view initiates a filesystem read only transaction
func view(db *bolt.DB, bucketpath string) (*fsTx, error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}

	b, err := openBucket(tx, bucketpath)
	if err != nil {
		return nil, err
	}
	return newFsTx(b, tx), nil

}

// Commit commits the filesystem transaction
func (f *fsTx) Commit() error {
	return f.tx.Commit()
}

// Rollback ends the filesystem transaction without change.
func (f *fsTx) Rollback() error {
	return f.tx.Rollback()
}

// NextInode returns the next iNode id (Ino), or an error.
func (f *fsTx) NextInode() (uint64, error) {
	return f.inodes.NextSequence()
}

// InodeInit initializes the `inodes` bucket if necessary. This is done
// because iNode 0 represents the `nil` iNode. An empty iNode is written to
// the `inodes` bucket to cause boltdb to increment the `NextSequence` counter
// to 1.
func (f *fsTx) InodeInit() error {
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
func (f *fsTx) LoadOrSet(key string, value []byte) ([]byte, error) {
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
func (f *fsTx) PutInode(ino uint64, node *iNode) error {
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
func (f *fsTx) GetInode(ino uint64) (*iNode, error) {
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
func (f *fsTx) Get(key string) ([]byte, error) {
	data := f.state.Get([]byte(key))
	if data == nil {
		return nil, os.ErrNotExist
	}
	return data, nil
}

// Put sets the state value for the given` key`.
func (f *fsTx) Put(key string, data []byte) error {
	return f.state.Put([]byte(key), data)
}

// Symlink creates or replaces the relative or absolute path associated with
// `ino`. The node identified by `ino` is not evaluated so care must be taken
// to only create symlinks to iNodes that already exist and have a Mode set to
// os.ModeSymlink.
//
// To remove a symlink set the path to an empty string"".
func (f *fsTx) Symlink(ino uint64, path string) error {
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
func (f *fsTx) Readlink(ino uint64) string {
	if ino == 0 {
		return ""
	}

	data := f.symlinks.Get(i2b(ino))
	if data == nil {
		return ""
	}

	return string(data)
}
