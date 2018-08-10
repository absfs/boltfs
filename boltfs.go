package boltfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	keypath "path"
	"strings"
	"time"

	"github.com/Avalanche-io/c4"
	log "github.com/Sirupsen/logrus"
	"github.com/absfs/absfs"
	bolt "github.com/coreos/bbolt"
)

/*
func (fs *Filer) Close() error
func New(name string) (bfs *Filer, err error)
func (fs *Filer) EvalPath(btx *bolt.Tx, path string) (id uint64, err error)
func (bfs *Filer) OpenFile(path string, flags int, mode os.FileMode) (file absfs.File, err error)
func (filer *Filer) Remove(path string) error
func (filer *Filer) RemoveAll(path string) error
func (filer *Filer) Stat(path string) (os.FileInfo, error)
func (filer *Filer) Mkdir(path string, perm os.FileMode) error
func (fs *Filer) MkdirAll(path string, perm os.FileMode) error
func (filer *Filer) Chmod(name string, mode os.FileMode) (err error)
func (filer *Filer) Chtimes(name string, atime time.Time, mtime time.Time) (err error)
func (filer *Filer) Chown(name string, uid, gid int) (err error)
func b2i(d []byte) uint32
func i2b(n uint32) []byte
func b2i64(d []byte) uint64
func i2b64(n uint64) []byte
*/

type Filer struct {
	db     *bolt.DB
	rootId uint64

	cwd string
}

func (fs *Filer) Close() error {
	return fs.db.Close()
}

func New(name string) (bfs *Filer, err error) {
	log.Infof("opening db: %q", name)
	db, err := bolt.Open(name, 0644, nil)
	if err != nil {
		return nil, err
	}
	bfs = &Filer{
		db: db,
	}

	err = db.Update(func(btx *bolt.Tx) error {

		// initialize buckets
		for _, name := range []string{"inodes", "dirs", "data", "labels"} {
			_, err := btx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}

		tx := &Tx{tx: btx}
		id := tx.GetLabel("latest")
		if id == 0 {
			id = tx.PutInode(newDirNode(0755))
		}
		bfs.rootId = id

		tx.SetLabel("latest", id)
		return tx.err
	})
	if err != nil {
		return nil, err
	}

	return bfs, nil
}

// EvalPath follows the given path and returns the ino of the last item
// the happens in a single bolt transaction
func (fs *Filer) EvalPath(btx *bolt.Tx, path string) (id uint64, err error) {
	if !strings.HasPrefix(path, "/") {
		return 0, &os.PathError{Op: "resolvePath", Err: os.ErrInvalid}
	}

	tx := &Tx{tx: btx}
	id = tx.GetLabel("latest")
	if id != fs.rootId {
		log.Fatalf("internal error %d, %d", id, fs.rootId)
	}

	pathstack := strings.Split(path, "/")
	pathstack[0] = "/"
	log.Infof("pathstack: %#v", pathstack)

	var cwd string

	for _, name := range pathstack {
		cwd = keypath.Join(cwd, name)

		dirs := tx.GetDirs(id)
		cid, ok := dirs.Find(name)
		if !ok {
			return 0, &os.PathError{Op: "EvalPath", Path: cwd, Err: os.ErrNotExist}
		}
		id = cid
	}

	return id, err
}

const O_ACCESS = 0x3

// func (tx *Tx) Open(filename string, flags int, mode os.FileMode) (f *dbFile, err error) {

func (bfs *Filer) Getwd() (string, error) {
	return bfs.cwd, nil
}

func (bfs *Filer) Paths(path string) (string, string, string) {
	if path == "/" {
		return path, "/", "/"
	}

	if !keypath.IsAbs(path) {
		path = keypath.Join(bfs.cwd, path)
		path = keypath.Clean(path)
	}

	dir, name := keypath.Split(path)
	dir = keypath.Clean(dir)
	return path, dir, name
}

func (bfs *Filer) OpenFile(filename string, flags int, mode os.FileMode) (file absfs.File, err error) {
	_, dir, name := bfs.Paths(filename)
	var cwd string
	cwd, err = bfs.Getwd()
	if dir == cwd {
		panic("not yet implemented")
	}

	var node *inode
	var parentID uint64
	var dirs Dir
	var data []byte
	var id uint64
	var create bool

	err = bfs.db.View(func(btx *bolt.Tx) error {
		var err error

		tx := NewTx(btx)
		parentID, err = tx.EvalPath(dir)
		if err != nil {
			return err
		}

		dirs = tx.GetDirs(parentID)
		log.Infof("\tdirs = %s", dirs)
		var exists bool
		id, exists = dirs.Find(name)

		if name == "/" {
			id = parentID
			exists = true
		}
		// error if it does not exist, and we are not allowed to create it.
		if !exists && (flags&os.O_CREATE == 0 || flags&O_ACCESS == os.O_RDONLY) {
			return &os.PathError{Op: "open", Path: filename, Err: os.ErrNotExist}
		}

		if exists {

			// err if exclusive create is required
			if flags&(os.O_CREATE|os.O_EXCL) != 0 {
				return &os.PathError{Op: "open", Path: filename, Err: os.ErrExist}
			}

			// load existing node
			node = tx.GetInode(id)
			log.Infof("\tnode (%d): %s", id, node)
			// Error if mode is not a regular file and the existing file is different
			if mode&os.ModeType != 0 && mode&os.ModeType != node.Mode&os.ModeType {
				return &os.PathError{Op: "open", Path: filename, Err: errors.New("conflicting file types")}
			}

			// if it is a directory, load entries
			if node.Mode&os.ModeDir != 0 {
				dirs = tx.GetDirs(id)
				log.Infof("\tdirs (%d): %s", id, dirs)
			}

			// if we must truncate the file
			if flags&os.O_TRUNC != 0 {
				log.Infof("\tos.O_TRUNC")

				// error if not writable
				if flags&O_ACCESS == os.O_RDONLY {
					log.Infof("os.O_TRUNC & os.O_RDONLY")
					return &os.PathError{Op: "open", Path: filename, Err: os.ErrPermission}
				}

				// if tx.err != nil {
				// 	panic(tx.err)
				// }

				// dropping link to node.Digest.(will need gc to cleanup)
				node.SetDigest(emptydigest)

				// write updated node
				tx.SetInode(id, node)

			} else {
				if tx.err != nil {
					panic(tx.err)
				}
				log.Infof("\tloading data from %q", filename)
				// load file data
				var n, offset int
				// var err error
				d := make([]byte, 4096)
				for err != io.EOF {
					n, err = tx.ReadData(node.Digest, offset, d)
					if err != nil && err != io.EOF {
						return err
					}
					offset += n
					data = append(data, d[:n]...)
				}
				if tx.err != nil {
					panic(tx.err)
				}
			}

		} else { // !exists

			// error if we cannot create the file
			if flags&os.O_CREATE == 0 {
				return &os.PathError{Op: "open", Path: filename, Err: os.ErrNotExist}
			}

			// create file

			// error if not writable
			if flags&O_ACCESS == os.O_RDONLY {
				log.Infof("\tos.O_RDONLY - non existing file")
				return &os.PathError{Op: "open", Path: filename, Err: os.ErrPermission}
			}

			// Create write-able file
			node = newNode(mode)

			create = true

		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if create && name != "/" {
		err = bfs.db.Update(func(btx *bolt.Tx) error {
			tx := NewTx(btx)

			id = tx.PutInode(node)
			if id == 0 {
				panic(fmt.Sprintf("id: 0 - indicates an internal error: %s", tx.err))
			}
			log.Infof("\tNew node (%d): %s", id, node)
			dirs = append(dirs, Entry{0, id, name})
			dirs.Sort()

			tx.SetDirs(parentID, dirs)
			return nil
		})
	}

	f := &File{
		db:    bfs.db,
		name:  filename,
		flags: flags,
		id:    id,
		node:  node,
		dirs:  dirs,
		data:  data,
		e:     c4.NewEncoder(),
	}

	return f, nil
}

func (filer *Filer) Remove(path string) error {
	return filer.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		return tx.Remove(path)
	})
}

func (filer *Filer) RemoveAll(path string) error {
	return filer.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		tx.PostOrder(path, func(path string, id uint64, err error) error {
			tx.delete(id)
			return nil
		})
		return nil
	})
}

func (filer *Filer) Stat(path string) (os.FileInfo, error) {
	var node *inode
	var name string

	err := filer.db.View(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		path, _, name = tx.processPath(path)
		id, err := tx.EvalPath(path)
		if err != nil {
			return err
		}
		node = tx.GetInode(id)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &FileInfo{name, node}, nil
}

func (filer *Filer) Mkdir(path string, perm os.FileMode) error {
	err := filer.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		path, _, _ := tx.processPath(path)
		tx.Mkdir(path, perm)
		return tx.err
	})
	return err
}

func (filer *Filer) MkdirAll(path string, perm os.FileMode) error {
	if !strings.HasPrefix(path, "/") {
		return ErrNotImplemented
	}

	pathstack := strings.Split(path, "/")
	pathstack[0] = "/"
	cwd := ""
	for _, name := range pathstack {
		cwd = keypath.Join(cwd, name)
		cwd = keypath.Clean(cwd)
		err := filer.Mkdir(cwd, 0755)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return err
		}
	}

	return nil
}

//Chmod changes the mode of the named file to mode.
func (filer *Filer) Chmod(name string, mode os.FileMode) (err error) {
	err = filer.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		path, _, _ := tx.processPath(name)

		id, err := tx.EvalPath(path)
		if err != nil {
			return err
		}
		node := tx.GetInode(id)
		node.Mode = node.Mode&os.ModeType | mode & ^os.ModeType
		now := time.Now().UTC()
		node.SetAccess(now)
		node.SetModified(now)
		tx.SetInode(id, node)
		return tx.err
	})
	return err
}

//Chtimes changes the access and modification times of the named file
func (filer *Filer) Chtimes(name string, atime time.Time, mtime time.Time) (err error) {
	err = filer.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		path, _, _ := tx.processPath(name)

		id, err := tx.EvalPath(path)
		if err != nil {
			return err
		}
		node := tx.GetInode(id)
		node.SetAccess(atime.UTC())
		node.SetModified(mtime.UTC())
		tx.SetInode(id, node)
		return tx.err
	})
	return err
}

var ErrNotImplemented = errors.New("not implemented")

//Chown changes the owner and group ids of the named file
func (filer *Filer) Chown(name string, uid, gid int) (err error) {
	return ErrNotImplemented
}

func b2i(d []byte) uint32 {
	return binary.LittleEndian.Uint32(d)
}

func i2b(n uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, n)
	return b
}

func b2i64(d []byte) uint64 {
	return binary.LittleEndian.Uint64(d)
}

func i2b64(n uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)
	return b
}
