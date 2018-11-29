package boltfs

import (
	"os"
	filepath "path"
	"strings"

	bolt "github.com/coreos/bbolt"
)

var buckets = []string{"state", "inodes", "data", "symlinks"}

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

func (f *fsBucket) NextInode() (uint64, error) {
	return f.inodes.NextSequence()
}

// InodeInit initializes the `inodes` bucket if nececary. This is done
// because iNode 0 reprezents the `nil` iNode. An empty iNode is written to
// the `inodes` bucket to cause boltdb to incroment the `NextSequence` counter
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

var nilIno uint64

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

func (f *fsBucket) Get(key string) ([]byte, error) {
	data := f.state.Get([]byte(key))
	if data == nil {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (f *fsBucket) Put(key string, data []byte) error {
	return f.state.Put([]byte(key), data)
}

func (f *fsBucket) Symlink(ino uint64, path string) error {
	if ino == 0 {
		return errNilNode
	}

	return f.symlinks.Put(i2b(ino), []byte(path))
}

func (f *fsBucket) Readlink(ino uint64) (string, error) {
	if ino == 0 {
		return "", errNilNode
	}

	data := f.symlinks.Get(i2b(ino))
	if data == nil {
		return "", os.ErrNotExist
	}

	return string(data), nil
}
