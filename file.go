package boltfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/absfs/absfs"
)

// File implements the absfs.File interface, providing a file interace for boltdb
type File struct {
	fs *FileSystem
	// db *bolt.DB

	name  string
	flags int
	node  *iNode

	offset    int64
	diroffset int
}

// Name returns the name of the file as presented to Open.
func (f *File) Name() string {
	return f.name
}

// Read reads up to len(b) bytes from the File. It returns the number of bytes
// read and any error encountered. At end of file, Read returns 0, io.EOF.
func (f *File) Read(p []byte) (int, error) {
	// if f == nil {
	// 	panic("nil file handle")
	// }
	if f.flags == 3712 {
		return 0, io.EOF
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EBADF} //os.ErrPermission
	}
	if f.node.IsDir() && f.node.Size == 0 {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EISDIR} //os.ErrPermission
	}
	if f.offset >= f.node.Size {
		return 0, io.EOF
	}

	data, err := f.fs.loadData(f.node.Ino)
	if err != nil {
		return 0, err
	}
	n := copy(p, data[f.offset:])
	f.offset += int64(n)
	// err := f.db.View(func(tx *bolt.Tx) error {
	// 	b := tx.Bucket([]byte("data"))
	// 	data := b.Get(i2b(f.node.Ino))
	// 	if data == nil {
	// 		return os.ErrNotExist
	// 	}
	// 	n = copy(p, data[f.offset:])
	// 	return nil
	// })

	if err != nil {
		return n, err
	}

	return n, nil
}

// ReadAt reads len(b) bytes from the File starting at byte offset off. It
// returns the number of bytes read and the error, if any. ReadAt always
// returns a non-nil error when n < len(b). At end of file, that error is
// io.EOF.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return 0, os.ErrPermission
	}
	f.offset = off
	return f.Read(b)
}

func (f *File) Write(p []byte) (int, error) {
	if f.flags&absfs.O_ACCESS == os.O_RDONLY {
		return 0, &os.PathError{Op: "write", Path: f.name, Err: syscall.EBADF}
	}

	size := len(p) + int(f.offset)
	data, err := f.fs.loadData(f.node.Ino)
	if err != nil {
		return 0, err
	}

	if size > len(data) {
		d := make([]byte, size)
		copy(d, data)
		data = d
	}

	n := copy(data[int(f.offset):], p)
	f.offset += int64(n)
	f.node.Size = int64(len(data))
	/*
		err := f.db.View(func(tx *bolt.Tx) error {

			// get from data bucket
			d := tx.Bucket([]byte("data")).Get(i2b(f.node.Ino))
			if d == nil {
				return nil
			}
			// if the saved data is longer then size / len(data) then create the larger
			// allocation
			if len(d) > len(data) {
				data = make([]byte, len(d))
			}
			copy(data, d)
			return nil
		})
		if err != nil {
			return  err
		}
	*/

	ino, err := f.fs.saveInode(f.node)
	if err != nil {
		return 0, err
	}

	err = f.fs.saveData(ino, data)
	if err != nil {
		return n, err
	}

	return n, nil
}

// WriteAt writes len(b) bytes to the File starting at byte offset off. It
// returns the number of bytes written and an error, if any. WriteAt returns
// a non-nil error when n != len(b).
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	f.offset = off
	return f.Write(b)
}

// Close closes the File, rendering it unusable for I/O. On files that support
// SetDeadline, any pending I/O operations will be canceled and return
// immediately with an error.
func (f *File) Close() error {
	err := f.Sync()
	if err != nil {
		return err
	}

	f.node = nil
	return nil
}

// Seek sets the offset for the next Read or Write on file to offset,
// interpreted according to whence: 0 means relative to the origin of the file,
// 1 means relative to the current offset, and 2 means relative to the end. It
// returns the new offset and an error, if any. The behavior of Seek on a file
// opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(f.node.Size) + offset
	}
	if f.offset < 0 {
		f.offset = 0
	}
	return f.offset, nil
}

// Stat returns the FileInfo structure describing file. If there is an error, it
// will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	return &fileinfo{filepath.Base(f.name), f.node}, nil
}

// Sync does nothing because Write always completes before returning
func (f *File) Sync() error {
	// if f.flags&absfs.O_ACCESS == os.O_RDONLY {
	// 	return nil
	// }
	// f.fs.data[int(f.node.Ino)] = f.data
	// f.node.Size = int64(len(f.data))
	return nil
}

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned by Lstat,
// in directory order. Subsequent calls on the same file will yield further
// FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case,
// if Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in a single
//  slice. In this case, if Readdir succeeds (reads all the way to the end of
// the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdir returns the FileInfo read
// until that point and a non-nil error.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return nil, os.ErrPermission
	}
	if !f.node.IsDir() {
		return nil, errors.New("not a directory")
	}
	children := f.node.Children
	if f.diroffset >= len(children) {
		return nil, io.EOF
	}
	if n < 1 {
		n = len(children)
	}
	infos := make([]os.FileInfo, n-f.diroffset)
	for i, entry := range children[f.diroffset:n] {
		node, err := f.fs.loadInode(entry.Ino)
		if err != nil {
			return nil, err
		}

		infos[i] = &fileinfo{entry.Name, node}
	}
	f.diroffset += n
	return infos, nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in a single
// slice. In this case, if Readdirnames succeeds (reads all the way to the end
// of the directory), it returns the slice and a nil error. If it encounters an
// error before the end of the directory, Readdirnames returns the names read
// until that point and a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	var list []string
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return list, os.ErrPermission
	}
	if !f.node.IsDir() {
		return list, errors.New("not a directory")
	}
	children := f.node.Children
	if f.diroffset >= len(children) {
		return list, io.EOF
	}
	if n < 1 {
		n = len(children)
	}
	list = make([]string, n-f.diroffset)
	for i, entry := range children[f.diroffset:n] {
		list[i] = entry.Name
	}
	f.diroffset += n
	return list, nil
}

// Truncate changes the size of the named file. If the file is a symbolic link,
// it changes the size of the link's target. If there is an error, it will be of
// type *PathError.
func (f *File) Truncate(size int64) error {
	if f.flags&absfs.O_ACCESS == os.O_RDONLY {
		return os.ErrPermission
	}
	data, err := f.fs.loadData(f.node.Ino)
	if err != nil {
		return err
	}

	if int(size) <= len(data) {
		data = data[:int(size)]
	} else {
		d := make([]byte, int(size))
		copy(data, d)
		data = d
	}

	ino := f.node.Ino
	if f.node.Size != size {
		f.node.Size = size
		_, err = f.fs.saveInode(f.node)
		if err != nil {
			return err
		}
	}

	return f.fs.saveData(ino, data)
}

// WriteString is like Write, but writes the contents of string s rather than a
// slice of bytes.
func (f *File) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}

type fileinfo struct {
	name string
	node *iNode
}

func (i *fileinfo) Name() string {
	return i.name
}

func (i *fileinfo) Size() int64 {
	return i.node.Size
}

func (i *fileinfo) ModTime() time.Time {
	return i.node.Mtime
}

func (i *fileinfo) Mode() os.FileMode {
	return i.node.Mode
}

func (i *fileinfo) Sys() interface{} {
	return i.node
}

func (i *fileinfo) IsDir() bool {
	return i.node.IsDir()
}

// type File struct {
// 	db        *bolt.DB
// 	name      string
// 	flags     int
// 	id        uint64
// 	node      *inode.Inode
// 	dirs      Directory
// 	data      []byte
// 	diroffset int
// 	offset    int64
// 	fileId    c4.ID
// }

// func (f *File) Name() string {
// 	return f.name
// }

// func (f *File) Read(b []byte) (n int, err error) {

// 	if int(f.flags&O_ACCESS) == os.O_WRONLY {
// 		return 0, os.ErrPermission
// 	}
// 	if f.offset >= int64(len(f.data)) {
// 		return 0, io.EOF
// 	}
// 	n = copy(b, f.data[f.offset:])

// 	f.offset += int64(n)
// 	return n, nil
// }

// func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
// 	_, err = f.Seek(off, io.SeekStart)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return f.Read(b)
// }

// func (f *File) Write(p []byte) (n int, err error) {
// 	if int(f.flags&O_ACCESS) == os.O_RDONLY {
// 		return 0, os.ErrPermission
// 	}
// 	f.node.SetAccess(time.Time{})
// 	f.node.SetModified(time.Time{})

// 	// if f.offset != int64(len(f.data)) {
// 	// 	f.e.Reset()
// 	// 	_, err = io.Copy(f.e, bytes.NewReader(f.data[:int(f.offset)]))
// 	// 	if err != nil {
// 	// 		return 0, err
// 	// 	}
// 	// }

// 	// _, err = io.Copy(f.e, bytes.NewReader(p))
// 	// if err != nil {
// 	// 	return 0, err
// 	// }

// 	if int64(len(f.data)) < f.offset+int64(len(p)) {
// 		data := make([]byte, int(f.offset+int64(len(p))))
// 		copy(data, f.data[:int(f.offset)])
// 		f.data = data
// 	}

// 	n = copy(f.data[int(f.offset):], p)
// 	f.offset += int64(n)
// 	f.node.Size = int64(len(f.data))
// 	f.fileId = c4.Identify(bytes.NewReader(f.data))
// 	return n, nil
// }

// func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
// 	_, err = f.Seek(off, io.SeekStart)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return f.Write(b)
// }

// func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
// 	switch whence {
// 	case io.SeekStart:
// 		f.offset = offset
// 	case io.SeekCurrent:
// 		f.offset += offset
// 	case io.SeekEnd:
// 		f.offset = int64(len(f.data)) + offset
// 	}
// 	if f.offset < 0 {
// 		f.offset = 0
// 	}
// 	return f.offset, nil
// }

// func (f *File) Stat() (os.FileInfo, error) {
// 	return &FileInfo{f.name, f.node}, nil
// }

// func (f *File) Sync() error {
// 	return ErrNotImplemented
// }

// func (f *File) Readdir(n int) (list []os.FileInfo, err error) {

// 	if f.dirs == nil {
// 		err = f.db.View(func(btx *bolt.Tx) error {
// 			tx := NewTx(btx)
// 			f.dirs = tx.GetDirs(f.id)
// 			return nil
// 		})
// 		if err != nil {
// 			return nil, err
// 		}
// 		if f.dirs == nil {
// 			f.dirs = Dir{}
// 		}
// 	}

// 	if len(f.dirs) == 0 {
// 		return nil, io.EOF
// 	}

// 	if n < 1 || n > len(f.dirs) {
// 		n = len(f.dirs)
// 	}
// 	var infos []os.FileInfo

// 	err = f.db.View(func(btx *bolt.Tx) error {
// 		tx := NewTx(btx)
// 		for _, entry := range f.dirs[:n] {
// 			if entry.Id == 0 {
// 				continue
// 			}
// 			node := tx.GetInode(entry.Id)
// 			if node == nil {
// 				continue
// 			}
// 			infos = append(infos, &FileInfo{entry.Name, node})
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	return infos, nil
// }

// func (f *File) Close() error {
// 	if int(f.flags&O_ACCESS) == os.O_RDONLY {
// 		return nil
// 	}

// 	f.node.SetID(f.fileId)

// 	return f.db.Update(func(btx *bolt.Tx) error {
// 		tx := NewTx(btx)
// 		err := tx.WriteData(f.node.Id, f.data)
// 		if err != nil {
// 			return err
// 		}

// 		if f.id == 0 {
// 			f.id = tx.PutInode(f.node)
// 		} else {
// 			tx.SetInode(f.id, f.node)
// 		}
// 		return nil
// 	})

// 	return nil
// }

// func (f *File) Readdirnames(n int) (list []string, err error) {
// 	var infos []os.FileInfo
// 	infos, err = f.Readdir(n)
// 	if err != nil {
// 		return nil, err
// 	}
// 	list = make([]string, len(infos))
// 	for i := range infos {
// 		list[i] = infos[i].Name()
// 	}

// 	return list, nil
// }

// func (f *File) Truncate(size int64) error {
// 	f.data = f.data[:int(size)]
// 	return nil
// }

// func (f *File) WriteString(s string) (int, error) {
// 	return f.Write([]byte(s))
// }

// type FileInfo struct {
// 	name string
// 	node *Inode
// }

// func (f *FileInfo) Name() string {
// 	return f.name
// }

// func (f *FileInfo) Mode() os.FileMode {
// 	return f.node.Mode
// }

// func (f *FileInfo) IsDir() bool {
// 	return f.Mode()&os.ModeDir != 0
// }

// func (f *FileInfo) ModTime() time.Time {
// 	return f.node.Mtime
// }

// func (f *FileInfo) Size() int64 {
// 	return f.node.Size
// }

// func (f *FileInfo) Sys() interface{} {
// 	return f.node
// }
