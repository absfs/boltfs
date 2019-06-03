package boltfs

import (
	"encoding/binary"
	"errors"
	"os"
	filepath "path"
	walkpath "path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/absfs/absfs"
)

var errNotDir = errors.New("not a directory")
var errNilIno = errors.New("ino is nil")
var errNoData = errors.New("no data")

// FileSystem implements absfs.FileSystem for the boltdb packages `github.com/coreos/bbolt`.
type FileSystem struct {
	db      *bolt.DB
	bucket  string
	rootIno uint64
	cwd     string

	// symlinks map[uint64]string
}

// NewFS creates a new FileSystem pointer in the convention of other `absfs`,
// implementations. It takes a bolt.DB pointer, and a bucket name to use
// as the storage location for the file system buckets. If `bucket` is an
// empty string file system buckets are created as top level buckets.
func NewFS(db *bolt.DB, bucketpath string) (*FileSystem, error) {

	// create buckets
	err := db.Update(func(tx *bolt.Tx) error {
		return bucketInit(tx, bucketpath)
	})
	if err != nil {
		return nil, err
	}

	// load or initialize
	rootIno := uint64(1)
	fs := &FileSystem{
		db:      db,
		bucket:  bucketpath,
		rootIno: rootIno,
		cwd:     "/",
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bb, err := openBucket(tx, bucketpath)
		if err != nil {
			return err
		}
		b := newFsBucket(bb)

		// create the `nil` node if it doesn't exist
		err = b.InodeInit()
		if err != nil {
			return err
		}

		// load the
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(0755))
		_, err = b.LoadOrSet("umask", data)
		if err != nil {
			return err
		}

		// load the root Ino if one is available
		data, err = b.LoadOrSet("rootIno", i2b(rootIno))
		if err != nil {
			return err
		}
		rootIno = b2i(data)

		_, err = b.GetInode(rootIno)
		if err == nil {
			return nil
		}

		if err == os.ErrNotExist {
			node := newInode(os.ModeDir | 0755)
			node.countUp()
			err = b.PutInode(rootIno, node)
			if err != nil {

				return err
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	fs.rootIno = rootIno
	return fs, nil

}

// Open takes an absolute or relative path to a `boltdb` file and an optionl
// bucket name to store boltfs buckets. If `bucket` is an empty string file
// system buckets are created as top level buckets. If the bolt database already
// exists it will be loaded, otherwise a new database is created with with
// default configuration.
func Open(path, bucketpath string) (*FileSystem, error) {

	// Open or create boltdb file.
	db, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}

	// create buckets
	err = db.Update(func(tx *bolt.Tx) error {
		return bucketInit(tx, bucketpath)
	})
	if err != nil {
		return nil, err
	}
	// load or initialize
	rootIno := uint64(1)

	err = db.Update(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)

		// create the `nil` node if it doesn't exist
		err := b.InodeInit()
		if err != nil {
			return err
		}

		// load the root Ino if one is available
		data, err := b.LoadOrSet("rootIno", i2b(rootIno))
		if err == nil {
			rootIno = b2i(data)
		}
		node, err := b.GetInode(rootIno)
		if err != nil {
			node = newInode(os.ModeDir | 0755)
			node.countUp()
			err = b.PutInode(rootIno, node)
		}

		return err
	})
	if err != nil {
		return nil, err
	}

	fs := &FileSystem{
		db:      db,
		bucket:  bucketpath,
		rootIno: rootIno,
		cwd:     "/",
	}

	return fs, nil
}

// Close waits for pending writes, then closes the database file.
func (fs *FileSystem) Close() error {
	return fs.db.Close()
}

// Umask returns the current `umaks` value. A non zero `umask` will be masked
// with file and directory creation permissions
func (fs *FileSystem) Umask() os.FileMode {
	var umask os.FileMode
	err := fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		data, err := b.Get("umask")
		if err != nil {
			return err
		}
		umask = os.FileMode(binary.BigEndian.Uint32(data))
		return nil
	})
	if err != nil {
		panic("don't panic! " + err.Error())
	}

	return umask
}

// SetUmask sets the current `umaks` value
func (fs *FileSystem) SetUmask(umask os.FileMode) {
	var data [4]byte

	err := fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)

		binary.BigEndian.PutUint32(data[:], uint32(umask))
		return b.Put("umask", data[:])
	})
	if err != nil {
		panic("don't panic! " + err.Error())
	}

}

// TempDir returns the path to a temporary directory
func (fs *FileSystem) TempDir() string {
	var tempdir string
	tempdir = "/tmp"
	err := fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		data := b.Get([]byte("tempdir"))
		if data != nil {
			tempdir = string(data)
			return nil
		}
		return b.Put([]byte("tempdir"), []byte(tempdir))
	})
	if err != nil {
		panic("don't panic!")
	}

	return tempdir
}

// SetTempdir sets the path to a temporary directory, but does not create the
// actual directories.
func (fs *FileSystem) SetTempdir(tempdir string) {
	fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		return b.Put([]byte("tempdir"), []byte(tempdir))
	})
}

// saveInode save an iNode to the databased.  If the iNode's `ino` number is
// non-zero the node will be saved with the `ino` provided.
// If `ino` is zero (the nil value) then a new `ino` is created. In both
// cases the `ino` value is returned.
func (fs *FileSystem) saveInode(node *iNode) (ino uint64, err error) {
	ino = node.Ino
	err = fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)

		// b := tx.Bucket([]byte("inodes"))
		if ino == 0 {
			ino, err = b.NextInode()
		}
		// ino, err = b.NextSequence()
		if err != nil {
			return err
		}
		return b.PutInode(ino, node)

	})
	return ino, err
}

var errInvalidIno = errors.New("invalid ino")

// saveSymlink saves a path to the Ino provided
// func (fs *FileSystem) Symlink(ino uint64, path string) error {
// 	if ino == 0 {
// 		return errInvalidIno
// 	}
// 	return fs.db.Update(func(tx *bolt.Tx) error {
// 		b := newFsBucket(tx, fs.bucket)
// 		return b.Symlink(ino, path)
// 		// return tx.Bucket([]byte("symlinks")).Put(i2b(ino), []byte(path))
// 	})
// }

// func (fs *FileSystem) Readlink(ino uint64) (string, error) {

// 	// saveSymlink saves a path to the Ino provided
// 	// func (fs *FileSystem) loadSymlink(ino uint64) (string, error) {
// 	// 	if ino == 0 {
// 	// 		return "", errInvalidIno
// 	// 	}
// 	var path string
// 	err := fs.db.View(func(tx *bolt.Tx) error {
// 		b := newFsBucket(tx, fs.bucket)
// 		var err error
// 		path, err = b.Readlink(ino)
// 		if err != nil {
// 			return err
// 		}
// 		return nil
// 	})
// 	if err != nil {
// 		return "", err
// 	}
// 	return path, nil
// }

// loadInode - loads the iNode defined by `ino` or returns an error
func (fs *FileSystem) loadInode(ino uint64) (*iNode, error) {
	if ino == 0 {
		return nil, errNilIno
	}

	node := new(iNode)
	err := fs.db.View(func(tx *bolt.Tx) error {
		return decodeNode(tx.Bucket([]byte("inodes")), ino, node)
	})

	return node, err
}

// saveData - saves file data for a given `ino` or returns an error
func (fs *FileSystem) saveData(ino uint64, data []byte) error {
	if ino == 0 {
		return errNilIno
	}

	return fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		return b.Put(i2b(ino), data)
	})
}

// loadData - loads file data for a given `ino` or returns an error
func (fs *FileSystem) loadData(ino uint64) ([]byte, error) {
	if ino == 0 {
		return nil, errNilIno
	}

	var data []byte
	err := fs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		d := b.Get(i2b(ino))
		if d == nil {
			d = []byte{}
		}
		data = make([]byte, len(d))
		copy(data, d)
		return nil
	})
	if err != nil {
		return data, err
	}

	return data, err
}

// cleanPath - takes the absolute or relative path provided by `name` and
// returns the directory and filename of the cleand absolute path.
func (fs *FileSystem) cleanPath(name string) (string, string) {
	path := name
	if !filepath.IsAbs(path) {
		path = filepath.Join(fs.cwd, path)
	}
	dir, filename := filepath.Split(path)
	dir = filepath.Clean(dir)
	return dir, filename
}

// Separator returns "/" as the seperator for this FileSystem
func (fs *FileSystem) Separator() uint8 {
	return '/'
}

// ListSeparator returns ":" as the seperator for this fileSystem
func (fs *FileSystem) ListSeparator() uint8 {
	return ':'
}

// Rename renames (moves) oldpath to newpath. If newpath already exists and
// is not a directory, Rename replaces it. OS-specific restrictions may apply
// when oldpath and newpath are in different directories. If there is an
// error, it will be of type *LinkError.
func (fs *FileSystem) Rename(oldpath, newpath string) error {
	linkErr := &os.LinkError{Op: "move", Old: oldpath, New: newpath}
	if oldpath == "/" {
		linkErr.Err = errors.New("the root folder may not be moved or renamed")
		return linkErr
	}

	srcDir, srcFilename := fs.cleanPath(oldpath)
	dstDir, dstFilename := fs.cleanPath(newpath)
	srcParent, srcChild := fs.loadParentChild(srcDir, srcFilename)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if srcParent == nil {
		linkErr.Err = os.ErrNotExist
		linkErr.Old = srcDir
		return linkErr
	}
	if srcChild == nil {
		linkErr.Err = os.ErrNotExist
		linkErr.Old = filepath.Join(srcDir, srcFilename)
		return linkErr
	}
	if dstChild != nil {
		linkErr.Err = os.ErrExist
		linkErr.New = filepath.Join(dstDir, dstFilename)
		return linkErr
	}

	_, err := dstParent.Link(dstFilename, srcChild.Ino)
	if err != nil {
		linkErr.Err = err
		return linkErr
	}

	_, err = fs.saveInode(dstParent)
	if err != nil {
		linkErr.Err = err
		return linkErr
	}

	_, err = srcParent.Unlink(srcFilename)
	if err != nil {
		linkErr.Err = err
		return linkErr
	}

	if dstChild != nil {
		dstChild.countDown()
	}
	_, err = fs.saveInode(srcParent)
	if err != nil {
		linkErr.Err = err
		return linkErr
	}

	// shouldn't have to
	// _, err = fs.saveInode(dstChild)
	// if err != nil {
	// 	linkErr.Err = err
	// 	return linkErr
	// }

	return nil
}

// Copy is a convenience funciton that duplicates the `source` path to the
// `newpath`
func (fs *FileSystem) Copy(source, destination string) error {
	pathErr := &os.PathError{Op: "copy", Path: source}
	if source == "/" {
		pathErr.Err = errors.New("the root folder may not be moved or renamed")
		return pathErr
	}

	srcDir, srcFilename := fs.cleanPath(source)
	dstDir, dstFilename := fs.cleanPath(destination)
	srcParent, srcChild := fs.loadParentChild(srcDir, srcFilename)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if srcParent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = srcDir
		return pathErr
	}
	if srcChild == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = filepath.Join(srcDir, srcFilename)
		return pathErr
	}
	if dstChild != nil {
		pathErr.Err = os.ErrExist
		pathErr.Path = filepath.Join(dstDir, dstFilename)
		return pathErr
	}

	node := copyInode(srcChild)
	node.Ino = 0

	ino, err := fs.saveInode(node)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = dstParent.Link(dstFilename, ino)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = fs.saveInode(dstParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	// _, err = srcParent.Unlink(srcFilename)
	// if err != nil {
	// 	pathErr.Err = err
	// 	return pathErr
	// }

	// if dstChild != nil {
	// 	dstChild.countDown()
	// }
	_, err = fs.saveInode(srcParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	// return errors.New("not implemented")
	return nil
}

// Chdir - changes the current directory to the absolute or relative path
// provided by `Chdir`
func (fs *FileSystem) Chdir(name string) error {
	dir, filename := fs.cleanPath(name)
	_, err := fs.resolve(dir)
	if err != nil {
		return err
	}
	fs.cwd = filepath.Join(dir, filename)
	return nil
}

// Getwd returns the current working directory, the error value is always `nil`.
func (fs *FileSystem) Getwd() (dir string, err error) {
	return fs.cwd, nil
}

// Open is a convenance function that opens a file in read only mode.
func (fs *FileSystem) Open(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

// Create is a convenance function that opens a file for reading and writting.
// If the file does not exist it is created, if it does then it is truncated.
func (fs *FileSystem) Create(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
}

// resolve resolves the path provided into a iNode, or an error
func (fs *FileSystem) resolve(path string) (*iNode, error) {
	node := new(iNode)
	// var data []byte

	err := fs.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte("inodes"))
		ino := fs.rootIno
		err := decodeNode(b, ino, node)
		if err != nil {
			return err
		}
		if path == "/" {
			return nil
		}

		for _, name := range strings.Split(strings.TrimLeft(path, "/"), "/") {
			// find the child's ino or error
			x := sort.Search(len(node.Children), func(i int) bool {
				return node.Children[i].Name >= name
			})
			if x == len(node.Children) || node.Children[x].Name != name {
				return os.ErrNotExist
			}

			// replace node with child or error
			n := new(iNode)
			err = decodeNode(b, node.Children[x].Ino, n)
			if err != nil {
				return err
			}
			node = n
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return node, nil

}

// OpenFile is the generalized open call; most users will use Open or Create
// instead. It opens the named file with specified flag (O_RDONLY etc.) and
// perm (before umask), if applicable. If successful, methods on the returned
// File can be used for I/O. If there is an error, it will be of type
// *os.PathError.
func (fs *FileSystem) OpenFile(name string, flag int, perm os.FileMode) (absfs.File, error) {
	file := &absfs.InvalidFile{Path: name}
	pathErr := &os.PathError{Op: "open", Path: name}

	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if parent == nil {
		pathErr.Err = os.ErrNotExist
		pathErr.Path = dir
		return file, pathErr
	}

	access := flag & absfs.O_ACCESS
	if dir == "/" && filename == "" {
		child = parent
	}

	if child == nil {
		// error if it does not exist, and we are not allowed to create it.
		if flag&os.O_CREATE == 0 {
			pathErr.Err = syscall.ENOENT
			return file, pathErr
		}

		// Create file
		child = newInode(perm &^ os.ModeType)
		err := fs.saveParentChild(parent, filename, child)
		if err != nil {
			pathErr.Err = err
			return file, pathErr
		}
	} else { // child exists
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			pathErr.Err = syscall.EEXIST
			return file, pathErr
		}

		if child.Mode.IsDir() {
			if access != os.O_RDONLY || flag&os.O_TRUNC != 0 {
				pathErr.Err = syscall.EISDIR
				return file, pathErr
			}
		}

		// if we must truncate the file
		if flag&os.O_TRUNC != 0 {
			err := fs.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("data"))
				return b.Put(i2b(child.Ino), []byte{})
			})
			if err != nil {
				pathErr.Err = err
				return file, pathErr
			}
			// fs.data[int(node.Ino)] = fs.data[int(node.Ino)][:0]
		}
	}

	if flag&os.O_CREATE == 0 {
		if access == os.O_RDONLY && child.Mode&absfs.OS_ALL_R == 0 ||
			access == os.O_WRONLY && child.Mode&absfs.OS_ALL_W == 0 ||
			access == os.O_RDWR && child.Mode&(absfs.OS_ALL_W|absfs.OS_ALL_R) == 0 {
			pathErr.Err = os.ErrPermission
			return file, pathErr
		}
	}
	return &File{fs: fs, name: name, flags: flag, node: child}, nil
}

// Stat returns the FileInfo structure describing file. If there is an error,
// it will be of type *os.PathError.
func (fs *FileSystem) Stat(name string) (os.FileInfo, error) {

	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(filepath.Join(dir, filename))
	if err != nil {
		return nil, err
	}

	if node.Mode&os.ModeSymlink == 0 {
		if filename == "" {
			filename = dir
		}
		return inodeinfo{filename, node}, nil
	}

	// link, err := fs.loadSymlink(node.Ino)
	// if err != nil {
	// 	return nil, err
	// }
	var link string
	err = fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		link = b.Readlink(node.Ino)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !filepath.IsAbs(link) {
		link = filepath.Join(name, link)
	}

	return fs.Stat(link)
}

// Truncate changes the size of the file. It does not change the I/O offset. If
// there is an error, it will be of type *os.PathError.
func (fs *FileSystem) Truncate(name string, size int64) error {
	dir, filename := fs.cleanPath(name)
	path := filepath.Join(dir, filename)
	node, err := fs.resolve(path)
	if err != nil {
		if err != os.ErrNotExist {
			return err
		}
		f, err := fs.Create(path)
		if err != nil {
			return err
		}
		f.Close()
		node, err = fs.resolve(path)
		if err != nil {
			return err
		}
	}

	return fs.db.Update(func(tx *bolt.Tx) error {

		// update the size of the inode
		node.Size = size
		err = encodeNode(tx.Bucket([]byte("inodes")), node.Ino, node)
		if err != nil {
			return err
		}

		// update the data
		b := tx.Bucket([]byte("data"))
		key := i2b(node.Ino)
		data := b.Get(key)

		d := make([]byte, int(size))
		if data != nil {
			copy(d, data)
		}

		return b.Put(key, d)
	})

}

// loadParentChild loads the node for `dir` and the child nodes with name
// `filename` or nil.
func (fs *FileSystem) loadParentChild(dir, filename string) (*iNode, *iNode) {
	if fs == nil {
		panic("receiver may not be nill")
	}
	filename = strings.Trim(filename, "/")

	if dir == "/" && filename == "" {
		node, err := fs.loadInode(fs.rootIno)
		if err != nil {
			return nil, nil
		}
		return node, nil
	}

	parent, err := fs.resolve(dir)
	if err != nil {
		return nil, nil
	}

	if !sort.IsSorted(parent.Children) {
		sort.Sort(parent.Children)
	}

	i := sort.Search(len(parent.Children), func(i int) bool {
		return parent.Children[i].Name >= filename
	})
	if i < len(parent.Children) && parent.Children[i].Name == filename {
		// found
		child, err := fs.loadInode(parent.Children[i].Ino)
		if err != nil {
			return parent, nil
		}
		return parent, child
	}

	// not found
	return parent, nil
}

func (fs *FileSystem) saveParentChild(parent *iNode, filename string, child *iNode) error {
	filename = strings.Trim(filename, "/")
	child.countUp()
	ino, err := fs.saveInode(child)
	if err != nil {
		return err
	}
	old, err := parent.Link(filename, ino)
	if err != nil {
		return err
	}
	if old != 0 {
		oldChild, err := fs.loadInode(old)
		if err != nil {
			return err
		}
		if oldChild.countDown() == 0 {
			fs.deleteInode(oldChild.Ino)
		}
		_, err = fs.saveInode(oldChild)
		if err != nil {
			return err
		}
	}

	_, err = fs.saveInode(parent)
	if err != nil {
		return err
	}
	return nil
}

func (fs *FileSystem) deleteInode(ino uint64) error {
	err := fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("inodes"))
		key := i2b(ino)
		err := b.Delete(key)
		if err != nil {
			return err
		}
		b = tx.Bucket([]byte("data"))
		b.Delete(key)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Mkdir creates a new directory with the specified name and permission bits (before umask).
// If there is an error, it will be of type *PathError.
func (fs *FileSystem) Mkdir(name string, perm os.FileMode) error {
	pathErr := &os.PathError{Op: "mkdir", Path: name}
	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if child != nil {
		pathErr.Err = syscall.EEXIST
		return pathErr
	}

	child = newInode(os.ModeDir | (perm &^ os.ModeType))
	err := fs.saveParentChild(parent, filename, child)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	return nil
}

// MkdirAll creates a directory named path, along with any necessary parents,
// and returns nil, or else returns an error. The permission bits perm (before
// umask) are used for all directories that MkdirAll creates. If path is already
// a directory, MkdirAll does nothing and returns nil.
func (fs *FileSystem) MkdirAll(name string, perm os.FileMode) error {
	dir, filename := fs.cleanPath(name)
	name = strings.TrimLeft(filepath.Join(dir, filename), "/")

	path := "/"
	for _, p := range strings.Split(name, "/") {
		path = filepath.Join(path, p)
		fs.Mkdir(path, perm)
	}

	return nil
}

// Remove removes the named file or (empty) directory. If there is an error, it
// will be of type *PathError.
func (fs *FileSystem) Remove(name string) error {

	// cannot remove the root
	if name == "/" {
		return nil
	}

	pathErr := &os.PathError{Op: "remove", Path: name}
	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, name)
	if child == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}

	if child.IsDir() && len(child.Children) > 0 {
		pathErr.Err = syscall.ENOTEMPTY
		return pathErr
	}

	_, err := parent.Unlink(filename)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = fs.saveInode(parent)
	if err != nil {
		return err
	}

	if child.countDown() == 0 {
		err = fs.deleteInode(child.Ino)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = fs.saveInode(child)
	return err
}

// Walk walks the file tree rooted at root, calling walkFn for each file or
// directory in the tree, including root. All errors that arise visiting files
// and directories are filtered by walkFn. The files are walked in lexical
// order, which makes the output deterministic but means that for very large
// directories Walk can be inefficient. Walk does not follow symbolic links.
func (fs *FileSystem) Walk(root string, fn func(string, os.FileInfo, error) error) error {

	dir, filename := fs.cleanPath(root)
	parent, node := fs.loadParentChild(dir, filename)
	root = filepath.Join(dir, filename)
	if node == nil {
		node = parent
	}

	if !node.IsDir() {
		return fn(root, inodeinfo{root, node}, nil)
	}
	ino := node.Ino

	var recurse func(string, uint64) error

	return fs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("inodes"))

		recurse = func(path string, ino uint64) error {
			node := new(iNode)
			err := decodeNode(b, ino, node)

			err = fn(path, inodeinfo{filepath.Base(path), node}, err)

			if err != nil {
				if err == walkpath.SkipDir {
					return nil
				}
				return err
			}
			if node == nil {
				return nil
			}

			for _, child := range node.Children {
				err := recurse(filepath.Join(path, child.Name), child.Ino)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return recurse(root, ino)
	})
}

// RemoveAll removes path and any children it contains. It removes everything
// it can but returns the first error it encounters. If the path does not exist,
// RemoveAll returns nil (no error).
func (fs *FileSystem) RemoveAll(name string) error {

	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	_, _ = parent, child
	var inos []uint64

	var rootid uint64
	if dir == "/" {
		rootid = parent.Ino
	}

	err := fs.Walk(filepath.Join(dir, filename), func(path string, info os.FileInfo, err error) error {
		node, ok := info.Sys().(*iNode)
		if !ok {
			return errors.New("unable to cast os.FileInfo to *iNode")
		}
		inos = append(inos, node.Ino)
		return nil
	})
	if err != nil {
		return err
	}

	for i, j := 0, len(inos)-1; i < len(inos)/2; i, j = i+1, j-1 {
		inos[i], inos[j] = inos[j], inos[i]
	}

	err = fs.db.Update(func(tx *bolt.Tx) error {
		nodeB := tx.Bucket([]byte("inodes"))
		dataB := tx.Bucket([]byte("data"))

		for _, ino := range inos {
			if rootid != 0 && ino == rootid {
				continue
			}
			key := i2b(ino)
			err := nodeB.Delete(key)
			if err != nil {
				return err
			}
			err = dataB.Delete(key)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	if child == nil {
		parent.Children = parent.Children[:0]
		fs.saveInode(parent)
	} else {
		child.Children = child.Children[:0]
		fs.saveInode(child)
	}

	return nil
}

//Chtimes changes the access and modification times of the named file
func (fs *FileSystem) Chtimes(name string, atime time.Time, mtime time.Time) error {
	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		return os.ErrNotExist
	}

	node.Atime = atime
	node.Mtime = mtime

	_, err := fs.saveInode(node)
	return err
}

//Chown changes the owner and group ids of the named file
func (fs *FileSystem) Chown(name string, uid, gid int) error {
	pathErr := &os.PathError{Op: "chown", Path: name}

	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}

	if node.Mode&os.ModeSymlink == 0 {
		node.Uid = uint32(uid)
		node.Gid = uint32(gid)

		_, err := fs.saveInode(node)
		if err != nil {
			pathErr.Err = nil
			return pathErr
		}
		return nil
	}
	var link string
	err := fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		link = b.Readlink(node.Ino)
		if link == "" {
			return os.ErrNotExist
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !filepath.IsAbs(link) {
		link = filepath.Join(name, link)
	}

	return fs.Chown(link, uid, gid)
}

//Chmod changes the mode of the named file to mode.
func (fs *FileSystem) Chmod(name string, mode os.FileMode) error {
	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		return os.ErrNotExist
	}

	node.Mode = mode

	_, err := fs.saveInode(node)
	return err
}

// Lstat returns a FileInfo describing the named file. If the file is a symbolic
// link, the returned FileInfo describes the symbolic link. Lstat makes no
// attempt to follow the link. If there is an error, it will be of type
// *PathError.
func (fs *FileSystem) Lstat(name string) (os.FileInfo, error) {
	pathErr := &os.PathError{Op: "lstat", Path: name}
	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(filepath.Join(dir, filename))
	if err != nil {
		pathErr.Err = err
		return nil, pathErr
	}
	if filename == "" {
		filename = dir
	}
	return inodeinfo{filename, node}, nil
}

// Lchown changes the numeric uid and gid of the named file. If the file is a
// symbolic link, it changes the uid and gid of the link itself. If there is an
// error, it will be of type *PathError.
//
// On Windows, it always returns the syscall.EWINDOWS error, wrapped in *PathError.
func (fs *FileSystem) Lchown(name string, uid, gid int) error {
	pathErr := &os.PathError{Op: "lchown", Path: name}
	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}
	node.Uid = uint32(uid)
	node.Gid = uint32(gid)

	_, err := fs.saveInode(node)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	return nil
}

// Readlink returns the destination of the named symbolic link. If there is an
// error, it will be of type *PathError.
func (fs *FileSystem) Readlink(name string) (string, error) {
	node, err := fs.resolve(name)
	if err != nil {
		return "", err
	}

	var link string
	err = fs.db.View(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		link = b.Readlink(node.Ino)
		if link == "" {
			return os.ErrNotExist
		}
		return nil
	})

	return link, nil
}

// Symlink creates newname as a symbolic link to oldname. If there is an error,
// it will be of type *LinkError.
func (fs *FileSystem) Symlink(source, destination string) error {
	pathErr := &os.PathError{Op: "symlink", Path: destination}

	dstDir, dstFilename := fs.cleanPath(destination)
	dstParent, dstChild := fs.loadParentChild(dstDir, dstFilename)

	if dstChild != nil {
		pathErr.Err = os.ErrExist
		pathErr.Path = destination
		return pathErr
	}

	node := newInode(os.ModeSymlink | (fs.Umask() &^ os.ModeType))

	ino, err := fs.saveInode(node)
	if err != nil {
		pathErr.Err = err
		pathErr.Path = destination
		return pathErr
	}

	err = fs.db.Update(func(tx *bolt.Tx) error {
		b := newFsBucket(tx)
		b.Symlink(ino, source)
		return nil
	})

	// err = fs.saveSymlink(ino, source)
	// if err != nil {
	// 	pathErr.Err = err
	// 	return pathErr
	// }
	// fmt.Printf("%d: %s -> %s\n", ino, destination, source)
	_, err = dstParent.Link(dstFilename, ino)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}
	ino, err = fs.saveInode(dstParent)
	if err != nil {
		pathErr.Err = err
		pathErr.Path = destination
		return pathErr
	}

	return nil
}
