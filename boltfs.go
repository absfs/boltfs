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

	bolt "github.com/coreos/bbolt"

	"github.com/absfs/absfs"
)

// O_ACCESS iss a mask for os.FileMode to get the access type,  os.O_RDONLY, os.O_WRONLY and os.O_RDWR
const O_ACCESS = 0x3

var errNotDir = errors.New("not a directory")
var errNilIno = errors.New("ino is nil")
var errNoData = errors.New("no data")

var buckets = []string{"state", "inodes", "data", "symlinks"}

// FileSystem implements absfs.FileSystem for the boltdb packages `github.com/coreos/bbolt`.
type FileSystem struct {
	db      *bolt.DB
	rootIno uint64
	cwd     string

	// symlinks map[uint64]string
}

// NewFS creates a new FileSystem pointer in the convention of other `absfs`,
// implementations. It takes an absolute or relative path to a `boltdb` file.
// If the file already exists it will be loaded, otherwise a new file with default configuration is created.
func NewFS(path string) (*FileSystem, error) {
	db, err := bolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			_, err := tx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	rootIno := uint64(1)

	db.Update(func(tx *bolt.Tx) error {
		state := tx.Bucket([]byte("state"))
		nodes := tx.Bucket([]byte("inodes"))
		node := new(iNode)

		ino, err := nodes.NextSequence()
		if err != nil {
			return err
		}
		if ino == 0 {
			node = newInode(0)
			node.Ino = ino
			node.countUp()
			err = encodeNode(nodes, rootIno, node)
			if err != nil {
				return err
			}
			ino, err = nodes.NextSequence()
			if err != nil {
				return err
			}
		}
		rootIno = ino
		data := state.Get([]byte("rootIno"))
		if data == nil {
			err = state.Put([]byte("rootIno"), i2b(rootIno))
			if err != nil {
				return err
			}
		} else {
			rootIno = binary.BigEndian.Uint64(data)
		}

		err = decodeNode(nodes, rootIno, node)
		if err != nil {
			if err == os.ErrNotExist {
				err = encodeNode(nodes, rootIno, node)
				node = newInode(os.ModeDir | 0755)
				node.Ino = rootIno
				node.countUp()
				err = encodeNode(nodes, rootIno, node)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}

		return nil
	})

	fs := &FileSystem{
		db:      db,
		rootIno: rootIno,
		cwd:     "/",
	}

	// tempdir := "/tmp"
	// umask := os.FileMode(0755)

	return fs, nil
}

// Close waits for pending writes, then closes the database file.
func (fs *FileSystem) Close() error {
	return fs.db.Close()
}

// Umask returns the current `umaks` value. A non zero `umask` will be masked with file and directory creation permissions
func (fs *FileSystem) Umask() os.FileMode {
	var umask os.FileMode
	umask = 0777
	err := fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		data := b.Get([]byte("umask"))
		if data != nil {
			umask = os.FileMode(binary.BigEndian.Uint32(data))
			return nil
		}
		data = make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(umask))

		return b.Put([]byte("umask"), data)
	})
	if err != nil {
		panic("don't panic! " + err.Error())
	}

	return umask
}

// SetUmask sets the current `umaks` value
func (fs *FileSystem) SetUmask(umask os.FileMode) {
	var data [4]byte

	fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))

		binary.BigEndian.PutUint32(data[:], uint32(umask))
		return b.Put([]byte("umask"), data[:])
	})
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

// SetTempdir sets the path to a temporary directory, but does not create the actual directorys
func (fs *FileSystem) SetTempdir(tempdir string) {
	fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("state"))
		return b.Put([]byte("tempdir"), []byte(tempdir))
	})
}

// createBuckets simply loops over the buckets provided and creates buckets of that name in the database
func createBuckets(tx *bolt.Tx, buckets []string) error {
	// initialize buckets
	for _, name := range buckets {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
	}
	return nil
}

// saveInode save an iNode to the databased.  If the iNode's ino number is non-zero the node will be saved with the ino number provided.
// If the Ino value is zero (the nil value) then a new ino is created. In both cases the ino value is returned.
func (fs *FileSystem) saveInode(node *iNode) (ino uint64, err error) {
	ino = node.Ino
	err = fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("inodes"))
		if ino == 0 {
			ino, err = b.NextSequence()
			if err != nil {
				return err
			}
			if ino == 0 {
				panic("nil Ino")
			}
			node.Ino = ino
		}

		return encodeNode(b, ino, node)
	})
	return ino, err
}

// saveInodes is a multi node version of saveInode. saveInodes attempts to create every inode before returnning the first error if any
func (fs *FileSystem) saveInodes(nodes ...*iNode) error {
	if len(nodes) == 0 {
		return nil
	}

	return fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("inodes"))

		for _, node := range nodes {
			ino := node.Ino
			var err error

			if ino == 0 {
				ino, err = b.NextSequence()
				if err != nil {
					ino = 0
					return err
				}
				if ino == 0 {
					encodeNode(b, 0, new(iNode))
					ino, err = b.NextSequence()
					if err != nil {
						ino = 0
						return err
					}
				}
				node.Ino = ino
			}

			err = encodeNode(b, ino, node)
			if err != nil {
				return err
			}
		}
		return nil

	})
}

// loadInode loads the iNode defined by ino, or an errors
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

//  saveData saves file data for a given ino *(iNode number)*
func (fs *FileSystem) saveData(ino uint64, data []byte) error {
	if ino == 0 {
		return errNilIno
	}

	return fs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		return b.Put(i2b(ino), data)
	})
}

// loadData loads file data for a given ino *(iNode number)*
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

//  cleanPath takes the absolute or relative path provided by `name` and returns the directory and filename of the cleand absolute path.
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

// Move is a convenience funciton that moves the `source` path to the `destination`,
// it will return an error if the `destination` path already exists
func (fs *FileSystem) Move(source, destination string) error {
	pathErr := &os.PathError{Op: "move", Path: source}
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

	_, err := dstParent.Link(dstFilename, srcChild.Ino)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = fs.saveInode(dstParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	_, err = srcParent.Unlink(srcFilename)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	if dstChild != nil {
		dstChild.countDown()
	}
	_, err = fs.saveInode(srcParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	// shouldn't have to
	// _, err = fs.saveInode(dstChild)
	// if err != nil {
	// 	pathErr.Err = err
	// 	return pathErr
	// }

	return nil
}

// Copy is a convenience funciton that duplicates the `source` path to the `destination`
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

	_, err = srcParent.Unlink(srcFilename)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	if dstChild != nil {
		dstChild.countDown()
	}
	_, err = fs.saveInode(srcParent)
	if err != nil {
		pathErr.Err = err
		return pathErr
	}

	// return errors.New("not implemented")
	return nil
}

// Chdir changes the current directory to the absolute or relative path provided by `Chdir`
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

// Create is a convenance function that opens a file for reading and writting. If the file does not
// exist it is created, if it does then it is truncated.
func (fs *FileSystem) Create(name string) (absfs.File, error) {
	return fs.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
}

// popPath removes first (or top) directory from the path provided and returns
// the name and the reamining portion of the path with any leading slashes removed.
func popPath(path string) (string, string) {
	if path == "" {
		return "", "" // 1
	}
	if path == "/" {
		return "/", "" // 2
	}

	x := strings.Index(path, "/")
	if x == -1 {
		return path, "" // 6
	} else if x == 0 {
		return "/", strings.TrimLeft(path, "/") // 3
	}
	return path[:x], strings.TrimLeft(path[x:], "/")
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

// OpenFile is the generalized open call; most users will use Open or Create instead.
// It opens the named file with specified flag (O_RDONLY etc.) and perm (before umask), if applicable.
// If successful, methods on the returned File can be used for I/O. If there is an error, it will be
// of type *os.PathError.
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

		// child = newInode(perm &^ os.ModeType)
		// child.countUp()
		// ino, err := fs.saveInode(child)
		// if err != nil {
		// 	pathErr.Err = err
		// 	return file, pathErr
		// }

		// _, err = parent.Link(filename, ino)
		// if err != nil {
		// 	pathErr.Err = err
		// 	return file, pathErr
		// }

		// _, err = fs.saveInode(parent)
		// if err != nil {
		// 	pathErr.Err = err
		// 	return file, pathErr
		// }
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

// Stat returns the FileInfo structure describing file. If there is an error, it will be of type *os.PathError.
func (fs *FileSystem) Stat(name string) (os.FileInfo, error) {
	dir, filename := fs.cleanPath(name)
	node, err := fs.resolve(filepath.Join(dir, filename))
	if err != nil {
		return nil, err
	}

	return inodeinfo{filename, node}, nil
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

//
func (fs *FileSystem) loadParentChild(dir, filename string) (*iNode, *iNode) {
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

	ino, err := findChild(parent, filename)
	if err != nil {
		return parent, nil
	}
	child := new(iNode)

	err = fs.db.View(func(tx *bolt.Tx) error {
		return decodeNode(tx.Bucket([]byte("inodes")), ino, child)
	})
	if err != nil {
		return parent, nil
	}
	return parent, child
}

func (fs *FileSystem) saveParentChild(parent *iNode, filename string, child *iNode) error {
	// TODO: turn these into manual transactions
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

// MkdirAll creates a directory named path, along with any necessary parents, and returns
// nil, or else returns an error. The permission bits perm (before umask) are used for all
// directories that MkdirAll creates. If path is already a directory, MkdirAll does nothing
// and returns nil. child = newInode(os.ModeDir | (fs.Umask()&perm)&^os.ModeType)
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

// Remove removes the named file or (empty) directory. If there is an error, it will be of type *PathError.
func (fs *FileSystem) Remove(name string) error {
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

// RemoveAll removes path and any children it contains. It removes everything it can but
// returns the first error it encounters. If the path does not exist, RemoveAll returns nil (no error).
func (fs *FileSystem) RemoveAll(name string) error {
	pathErr := &os.PathError{Op: "remove", Path: name}
	dir, filename := fs.cleanPath(name)
	parent, child := fs.loadParentChild(dir, filename)
	if child == nil {
		pathErr.Err = os.ErrNotExist
		return pathErr
	}
	var inos []uint64
	err := fs.Walk(name, func(path string, info os.FileInfo, err error) error {
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

	err = fs.db.Update(func(tx *bolt.Tx) error {
		nodeB := tx.Bucket([]byte("inodes"))
		dataB := tx.Bucket([]byte("data"))

		for _, ino := range inos {
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

	_, err = parent.Unlink(filename)
	if err != nil {
		return err
	}
	if child != nil {
		if child.countDown() == 0 {
			fs.deleteInode(child.Ino)
		} else {
			_, err = fs.saveInode(child)
			if err != nil {
				return err
			}
		}
	}

	_, err = fs.saveInode(parent)

	return err
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
	dir, filename := fs.cleanPath(name)
	_, node := fs.loadParentChild(dir, filename)
	if node == nil {
		return os.ErrNotExist
	}

	node.Uid = uint32(uid)
	node.Gid = uint32(gid)

	_, err := fs.saveInode(node)
	return err
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
