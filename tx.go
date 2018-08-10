package boltfs

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"os"
	keypath "path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Avalanche-io/c4"
	log "github.com/Sirupsen/logrus"
	bolt "github.com/coreos/bbolt"
)

type Tx struct {
	tx     *bolt.Tx
	labels *bolt.Bucket
	dirs   *bolt.Bucket
	inodes *bolt.Bucket
	data   *bolt.Bucket
	err    error
	digest c4.Digest

	cwd   string
	cid   uint64
	cwn   *inode
	cwdir Dir
}

func NewTx(btx *bolt.Tx) *Tx {
	tx := &Tx{tx: btx}

	return tx
}

func (tx *Tx) processPath(path string) (string, string, string) {
	if path == "/" {
		return path, "/", "/"
	}

	if !keypath.IsAbs(path) {
		path = keypath.Join(tx.cwd, path)
		path = keypath.Clean(path)
	}

	dir, name := keypath.Split(path)
	dir = keypath.Clean(dir)
	return path, dir, name
}

func (tx *Tx) Getwd() (dir string, err error) {
	return tx.cwd, nil
}

func (tx *Tx) Chdir(path string) error {
	if tx.err != nil {
		return tx.err
	}

	path, _, _ = tx.processPath(path)

	if path == tx.cwd {
		return nil
	}

	id, err := tx.EvalPath(path)
	if err != nil {
		return err
	}

	node := tx.GetInode(id)
	dirs := tx.GetDirs(id)
	if tx.err != nil {
		return tx.err
	}

	tx.cwd = path
	tx.cid = id
	tx.cwn = node
	tx.cwdir = dirs
	return nil
}

func (tx *Tx) Mkdir(path string, mode os.FileMode) (id uint64) {
	if tx.err != nil {
		return 0
	}

	dir := keypath.Dir(path)
	name := keypath.Base(path)

	parentID, err := tx.EvalPath(dir)
	if err != nil {
		tx.err = err
		return 0
	}
	dirs := tx.GetDirs(parentID)

	_, ok := dirs.Find(name)
	if ok {
		tx.err = os.ErrExist
		return 0
	}
	id = tx.PutInode(newDirNode(mode))
	if id == 0 {
		panic("id: 0 - indicates an internal error")
	}
	dirs = append(dirs, Entry{0, id, name})
	dirs.Sort()
	tx.SetDirs(parentID, dirs)
	return id
}

func (tx *Tx) GetLabel(tag string) (id uint64) {
	if tx.err != nil {
		return
	}

	if tx.labels == nil {
		tx.labels = tx.tx.Bucket([]byte("labels"))
	}

	if tx == nil {
		panic("tx == nil")
	}
	if tx.labels == nil {
		panic("tx.labels == nil")
	}

	d := tx.labels.Get([]byte(tag))
	if d == nil {
		return 0
	}

	id = b2i64(d)
	return id
}

func (tx *Tx) SetLabel(tag string, id uint64) {

	if tx.err != nil {
		return
	}
	if id == 0 {
		panic("may not be zero")
	}
	if tx.labels == nil {
		tx.labels = tx.tx.Bucket([]byte("labels"))
	}

	err := tx.labels.Put([]byte(tag), i2b64(id))
	if err != nil {
		tx.err = err
	}
}

func (tx *Tx) SetDirs(id uint64, dirs Dir) {
	if tx.err != nil {
		return
	}

	if tx.dirs == nil {
		tx.dirs = tx.tx.Bucket([]byte("dirs"))
	}

	err := tx.dirs.Put(i2b64(id), dirs.Bytes())
	if err != nil {
		panic(err)
	}
}

func (tx *Tx) GetDirs(id uint64) (dirs Dir) {
	// log.Infof("GetDirs: %d", id)
	if id == tx.cid {
		return tx.cwdir
	}

	if tx.err != nil {
		return
	}

	if tx.dirs == nil {
		tx.dirs = tx.tx.Bucket([]byte("dirs"))
	}

	data := tx.dirs.Get(i2b64(id))
	if data == nil {
		return nil
	}

	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&dirs)
	if err != nil {
		tx.err = err
	}

	return dirs
}

func (tx *Tx) GetInode(id uint64) (node *inode) {
	if tx.err != nil {
		return nil
	}

	if tx.inodes == nil {
		tx.inodes = tx.tx.Bucket([]byte("inodes"))
	}

	data := tx.inodes.Get(i2b64(id))
	if data == nil {
		return nil
	}

	node = new(inode)
	err := node.UnmarshalBinary(data)
	if err != nil {
		tx.err = err
	}

	return node
}

func (tx *Tx) SetInode(id uint64, node *inode) {
	if tx.err != nil {
		return
	}

	if tx.inodes == nil {
		tx.inodes = tx.tx.Bucket([]byte("inodes"))
	}

	err := tx.inodes.Put(i2b64(id), node.Bytes())
	if err != nil {
		tx.err = err
	}

	return
}

func (tx *Tx) PutInode(node *inode) (id uint64) {
	if tx.err != nil {
		panic("tx.err " + tx.err.Error())
		return 0
	}

	if tx.inodes == nil {
		tx.inodes = tx.tx.Bucket([]byte("inodes"))
	}

	var err error
	id, err = tx.inodes.NextSequence()
	if err != nil {
		tx.err = err
		return 0
	}

	/*
		if id == 0 {
			id, err = tx.inodes.NextSequence()
			if err != nil {
				tx.err = err
				return 0
			}
			if id == 0 {
				panic("can't do that!")
			}
			// tx.SetInode(id, new(inode))
			// id = tx.inodes.NextSequence()
			// if id == 0 {
			// 	panic("id cannot be zero value")
			// }
		}
	*/
	tx.SetInode(id, node)
	return id
}

func (tx *Tx) EvalPath(path string) (id uint64, err error) {
	if path == tx.cwd || path == "" {
		return tx.cid, nil
	}
	if !strings.HasPrefix(path, "/") {
		path = keypath.Join(tx.cwd, path)
		path = keypath.Clean(path)
	}

	pathstack := strings.Split(path, "/")
	pathstack[0] = "/"

	id = tx.GetLabel("latest")
	if id == 0 {
		panic("id may not be zero")
	}
	if path == "/" {
		return id, nil
	}
	// dirs := tx.GetDirs(id)

	cwd := pathstack[0]
	for _, name := range pathstack[1:] {
		dirs := tx.GetDirs(id)
		childId, ok := dirs.Find(name)
		if !ok {
			return id, &os.PathError{Op: "EvalPath", Path: cwd, Err: os.ErrNotExist}
		}
		cwd = filepath.Join(cwd, name)

		id = childId
	}

	return id, nil

}

func (tx *Tx) ReadData(digest c4.Digest, offset int, b []byte) (n int, err error) {
	if tx.err != nil {
		return 0, tx.err
	}
	if tx.data == nil {
		tx.data = tx.tx.Bucket([]byte("data"))
	}

	data := tx.data.Get(digest)
	if data == nil || offset >= len(data) {
		return 0, io.EOF
	}

	n = copy(b, data[offset:])
	return n, nil
}

func (tx *Tx) DataExists(digest c4.Digest) bool {
	if tx.data == nil {
		tx.data = tx.tx.Bucket([]byte("data"))
	}
	data := tx.data.Get(digest)
	if data == nil {
		return false
	}
	return true
}

func (tx *Tx) WriteData(digest c4.Digest, b []byte) error {
	if tx.err != nil {
		return tx.err
	}

	if tx.data == nil {
		tx.data = tx.tx.Bucket([]byte("data"))
	}

	return tx.data.Put(digest, b)
}

type dbFile struct {
	tx      *Tx
	id      uint64
	node    *inode
	offset  int64
	data    []byte
	e       *c4.Encoder
	flags   int
	dirs    Dir
	doffset int64
}

func (tx *Tx) PreOrder(root string, fn func(path string, id uint64, err error) error) error {
	root, _, name := tx.processPath(root)
	id, err := tx.EvalPath(root)
	if err != nil {
		return err
	}

	var stack []Entry
	push := func(e Entry) {
		stack = append(stack, e)
	}
	pop := func() (e Entry) {
		e = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return e
	}
	empty := func() bool {
		return len(stack) > 0
	}
	push(Entry{0, id, name})

	for !empty() {
		e := pop()
		err = fn(e.Name, e.Id, nil)
		if err != nil {
			if err != filepath.SkipDir {
				continue
			}
			return err
		}
		dirs := tx.GetDirs(id)
		if len(dirs) == 0 {
			continue
		}
		sort.Sort(sort.Reverse(dirs))

		for _, entry := range dirs {
			push(entry)
		}
	}

	return nil
}

func (tx *Tx) PostOrder(root string, fn func(path string, id uint64, err error) error) error {

	root, _, name := tx.processPath(root)
	id, err := tx.EvalPath(root)
	if err != nil {
		return err
	}

	//	1.1 Create an empty stack
	var stack []*Entry
	push := func(e *Entry) {
		stack = append(stack, e)
	}
	pop := func() (e *Entry) {
		if len(stack) == 0 {
			return nil
		}
		e = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		return e
	}
	// peek := func() (e *Entry) {
	// 	if len(stack) == 0 {
	// 		return nil
	// 	}
	// 	return stack[len(stack)-1]
	// }
	empty := func() bool {
		return len(stack) == 0
	}
	count := 0
	node := &Entry{0, id, name}
	// log.Infof("!(empty() && node == nil) == !(%t && %t) == %t", empty(), node == nil, !(empty() && node == nil))
	for !(empty() && node == nil) {
		count++
		if count > 100 {
			break
		}
		//	2.1 Do following while `node` is not NULL
		if node != nil {
			// log.Infof("node: %s", node)
			//		a) Push nods's right children and then node to stack.
			dirs := tx.GetDirs(node.Id)
			sort.Sort(sort.Reverse(dirs))
			if len(dirs) == 0 {
				// log.Infof("len(dirs) == 0, empty() %t, stack: %s", empty(), stack)
				push(node)
				node = nil
				continue
			}
			for i := range dirs {
				dirs[i].SetDepth(node.Depth() + 1)
				push(&dirs[i])
			}
			left := pop()
			push(node)

			//		b) Set `node` as `node`'s left child.
			node = left
			// log.Infof("stack: %s", stack)
			continue
		}
		if empty() {
			break
		}
		// log.Infof("node == nil stack: %s", stack)

		//	2.2 Pop an item from stack and set it as `node`.
		node = pop()
		//		a) If the popped item has a right child and the right child
		//			is at top of stack, then remove the right child from stack,
		//			push the root back and set root as root's right child.

		right := pop()
		if right != nil && right.Depth() == node.Depth()+1 {
			// log.Infof("right.Depth() %d == node.Depth() %d +1 %q, %q", right.Depth(), node.Depth(), right.Name, node.Name)
			push(node)
			node = right
			continue
		}
		if right != nil {
			push(right)
		}

		//		b) Else print root's data and set root as NULL.
		if node != nil {
			err := fn(node.Name, node.Id, nil)
			if err != nil {
				return err
			}
		}

		node = nil

	} //	2.3 Repeat steps 2.1 and 2.2 while stack is not empty.
	// push(&Entry{0, id, name})

	// for !empty() {
	// 	e := peek()
	// 	dirs := tx.GetDirs(e.Id)
	// 	if dirs.Len() == 0 {
	// 		err = fn(e.Name, e.Id, nil)
	// 	}
	// 	sort.Sort(sort.Reverse(dirs))
	// 	for i := range dirs {
	// 		dirs[i].SetDepth(e.Depth() + 1)
	// 		push(&dirs[i])
	// 	}

	// 	err = fn(e.Name, e.Id, nil)
	// 	if err != nil {
	// 		if err != filepath.SkipDir {
	// 			continue
	// 		}
	// 		return err
	// 	}
	// 	dirs = tx.GetDirs(id)
	// 	if len(dirs) == 0 {
	// 		continue
	// 	}
	// 	sort.Sort(sort.Reverse(dirs))

	// 	for _, entry := range dirs {
	// 		entry.SetDepth(e.Depth() + 1)
	// 		push(&entry)
	// 	}
	// }
	return nil
}

func (tx *Tx) delete(id uint64) {
	if tx.data == nil {
		tx.data = tx.tx.Bucket([]byte("data"))
	}

	if tx.dirs == nil {
		tx.dirs = tx.tx.Bucket([]byte("dirs"))
	}

	// node := tx.GetInode(id)

	// delete data
	// can't do this just yet, because it may be pointed to by other inodes
	// err := tx.data.Delete(node.Digest)
	// if err != nil {
	// 	tx.err = err
	// 	return
	// }

	// delete dirs
	err := tx.dirs.Delete(i2b64(id))
	if err != nil {
		tx.err = err
		return
	}

	// delete inode
	err = tx.inodes.Delete(i2b64(id))
	if err != nil {
		tx.err = err
		return
	}
}

func (tx *Tx) Remove(filename string) error {
	path, _, _ := tx.processPath(filename)

	id, err := tx.EvalPath(path)
	if err != nil {
		return &os.PathError{Op: "remove", Path: filename, Err: os.ErrNotExist}
	}
	node := tx.GetInode(id)
	if node.Mode&os.ModeDir == 0 {
		tx.delete(id)
		return nil
	}
	// non-regular file
	dirs := tx.GetDirs(id)
	if dirs.Len() > 0 {
		return errors.New("directory not empty")
	}

	tx.delete(id)
	return nil
}

func (tx *Tx) Open(filename string, flags int, mode os.FileMode) (f *dbFile, err error) {
	if tx.err != nil {
		return nil, tx.err
	}

	_, dir, name := tx.processPath(filename)

	var cwd string
	cwd, err = tx.Getwd()
	if dir == cwd {
		panic("not yet implemented")
	}

	var node *inode
	var parentID uint64
	var dirs Dir
	var data []byte

	parentID, err = tx.EvalPath(dir)
	if err != nil {
		return nil, err
	}

	dirs = tx.GetDirs(parentID)
	id, exists := dirs.Find(name)

	// error if it does not exist, and we are not allowed to create it.
	if !exists && (flags&os.O_CREATE == 0 || flags&O_ACCESS == os.O_RDONLY) {
		return nil, &os.PathError{Op: "open", Path: filename, Err: os.ErrNotExist}
	}

	if exists {

		// err if exclusive create is required
		if flags&(os.O_CREATE|os.O_EXCL) != 0 {
			return nil, &os.PathError{Op: "open", Path: filename, Err: os.ErrExist}
		}

		// load existing node
		node = tx.GetInode(id)

		// Error if mode is not a regular file and the existing file is different
		if mode&os.ModeType != 0 && mode&os.ModeType != node.Mode&os.ModeType {
			return nil, &os.PathError{Op: "open", Path: filename, Err: errors.New("conflicting file types")}
		}

		// if it is a directory, load entries
		if node.Mode&os.ModeDir != 0 {
			dirs = tx.GetDirs(id)
		}

		// if we must truncate the file
		if flags&os.O_TRUNC != 0 {

			// error if not writable
			if flags&O_ACCESS == os.O_RDONLY {
				log.Infof("tx os.O_RDONLY non existing file ")
				return nil, &os.PathError{Op: "open", Path: filename, Err: os.ErrPermission}
			}
			if tx.err != nil {
				panic(tx.err)
			}
			// dropping link to node.Digest.(will need gc to cleanup)
			node.SetDigest(emptydigest)

			// write updated node
			tx.SetInode(id, node)

		} else {
			if tx.err != nil {
				panic(tx.err)
			}
			// load file data
			var n, offset int
			var err error
			d := make([]byte, 4096)
			for err != io.EOF {
				n, err = tx.ReadData(node.Digest, offset, d)
				if err != nil && err != io.EOF {
					return nil, err
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
			return nil, &os.PathError{Op: "open", Path: filename, Err: os.ErrNotExist}
		}

		// create file

		// error if not writable
		if flags&O_ACCESS == os.O_RDONLY {
			return nil, &os.PathError{Op: "open", Path: filename, Err: os.ErrPermission}
		}

		// Create write-able file
		node = newNode(mode)

		id = tx.PutInode(node)
		if id == 0 {
			panic("id: 0 - indicates an internal error")
		}

		dirs = append(dirs, Entry{0, id, name})
		dirs.Sort()

		tx.SetDirs(parentID, dirs)

	}

	f = &dbFile{
		tx:    tx,
		id:    id,
		node:  node,
		e:     c4.NewEncoder(),
		flags: flags,
		data:  data,
		dirs:  dirs,
	}

	return f, nil
}

func (f *dbFile) Write(b []byte) (n int, err error) {
	if int(f.flags&O_ACCESS) == os.O_RDONLY {
		return 0, os.ErrPermission
	}
	f.node.SetAccess(time.Time{})
	f.node.SetModified(time.Time{})

	if f.offset != int64(len(f.data)) {
		f.e.Reset()
		_, err = io.Copy(f.e, bytes.NewReader(f.data[:int(f.offset)]))
		if err != nil {
			return 0, err
		}
	}

	_, err = io.Copy(f.e, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}

	if int64(len(f.data)) < f.offset+int64(len(b)) {
		data := make([]byte, int(f.offset+int64(len(b))))
		copy(data, f.data[:int(f.offset)])
		f.data = data
	}

	n = copy(f.data[int(f.offset):], b)
	f.offset += int64(n)
	f.node.Size = int64(len(f.data))

	return n, nil
}

func (f *dbFile) Read(b []byte) (n int, err error) {
	if int(f.flags&O_ACCESS) == os.O_WRONLY {
		return 0, os.ErrPermission
	}

	n, err = f.tx.ReadData(f.node.Digest, int(f.offset), b)
	f.offset += int64(n)
	return n, err
}

func (f *dbFile) Close() error {
	if int(f.flags&O_ACCESS) == os.O_RDONLY {
		return nil
	}

	f.node.SetDigest(f.e.Digest())

	err := f.tx.WriteData(f.node.Digest, f.data)
	if err != nil {
		return err
	}
	if f.id == 0 {
		f.id = f.tx.PutInode(f.node)
		if f.id == 0 {
			panic("id: 0 - indicates an internal error")
		}
	} else {
		f.tx.SetInode(f.id, f.node)
	}
	return nil
}

func (f *dbFile) Seek(offset int64, whence int) (n int64, err error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.data)) + offset
	}
	return f.offset, nil
}
