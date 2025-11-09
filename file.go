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

// dirWriteFlags represents the flag combination (07200 octal) for a directory
// opened with write permissions, which should return EOF when read.
const dirWriteFlags = 3712

// File implements the absfs.File interface, providing a file interface for boltdb
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

	if f.flags == dirWriteFlags {
		return 0, io.EOF
	}
	if f.flags&absfs.O_ACCESS == os.O_WRONLY {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EBADF} //os.ErrPermission
	}
	if f.node.IsDir() && f.node.Size == 0 {
		return 0, &os.PathError{Op: "read", Path: f.name, Err: syscall.EISDIR} //os.ErrPermission
	}
	offset := int(f.offset)

	data, err := f.fs.loadData(f.node.Ino)
	if err != nil {
		return 0, err
	}

	// TODO: validate info.Size()
	if offset >= len(data) {
		return 0, io.EOF
	}

	n := copy(p, data[f.offset:])
	f.offset += int64(n)

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
//
//	slice. In this case, if Readdir succeeds (reads all the way to the end of
//
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

	// Calculate how many entries to return
	remaining := len(children) - f.diroffset
	if n < 1 {
		n = remaining
	} else if n > remaining {
		n = remaining
	}

	infos := make([]os.FileInfo, n)
	endOffset := f.diroffset + n
	for i, entry := range children[f.diroffset:endOffset] {
		node, err := f.fs.loadInode(entry.Ino)
		if err != nil {
			return nil, err
		}

		infos[i] = &fileinfo{entry.Name, node}
	}
	f.diroffset = endOffset
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
	if n < 1 || len(children[f.diroffset:]) < n {
		n = len(children[f.diroffset:])
	}

	list = make([]string, n)

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
