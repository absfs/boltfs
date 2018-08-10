package boltfs

import (
	"bytes"
	"io"
	"os"
	"time"

	"github.com/Avalanche-io/c4"
	log "github.com/Sirupsen/logrus"
	bolt "github.com/coreos/bbolt"
)

type File struct {
	db        *bolt.DB
	name      string
	flags     int
	id        uint64
	node      *inode
	dirs      Dir
	data      []byte
	diroffset int
	offset    int64
	e         *c4.Encoder
}

func (f *File) Name() string {
	return f.name
}

func (f *File) Read(b []byte) (n int, err error) {

	if int(f.flags&O_ACCESS) == os.O_WRONLY {
		return 0, os.ErrPermission
	}
	if f.offset >= int64(len(f.data)) {
		log.Infof("Read size, offset %d, %d", len(f.data), f.offset)
		return 0, io.EOF
	}
	n = copy(b, f.data[f.offset:])

	f.offset += int64(n)
	return n, nil
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return f.Read(b)
}

func (f *File) Write(p []byte) (n int, err error) {
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

	_, err = io.Copy(f.e, bytes.NewReader(p))
	if err != nil {
		return 0, err
	}

	if int64(len(f.data)) < f.offset+int64(len(p)) {
		data := make([]byte, int(f.offset+int64(len(p))))
		copy(data, f.data[:int(f.offset)])
		f.data = data
	}

	n = copy(f.data[int(f.offset):], p)
	f.offset += int64(n)
	f.node.Size = int64(len(f.data))

	return n, nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}

	return f.Write(b)
}

func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.data)) + offset
	}
	if f.offset < 0 {
		f.offset = 0
	}
	return f.offset, nil
}

func (f *File) Stat() (os.FileInfo, error) {
	return &FileInfo{f.name, f.node}, nil
}

func (f *File) Sync() error {
	return ErrNotImplemented
}

func (f *File) Readdir(n int) (list []os.FileInfo, err error) {

	if f.dirs == nil {
		err = f.db.View(func(btx *bolt.Tx) error {
			tx := NewTx(btx)
			f.dirs = tx.GetDirs(f.id)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if f.dirs == nil {
			f.dirs = Dir{}
		}
	}

	if len(f.dirs) == 0 {
		return nil, io.EOF
	}

	if n < 1 || n > len(f.dirs) {
		n = len(f.dirs)
	}
	var infos []os.FileInfo

	err = f.db.View(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		for _, entry := range f.dirs[:n] {
			if entry.Id == 0 {
				continue
			}
			node := tx.GetInode(entry.Id)
			if node == nil {
				continue
			}
			infos = append(infos, &FileInfo{entry.Name, node})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (f *File) Close() error {
	if int(f.flags&O_ACCESS) == os.O_RDONLY {
		return nil
	}

	f.node.SetDigest(f.e.Digest())

	return f.db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		err := tx.WriteData(f.node.Digest, f.data)
		if err != nil {
			return err
		}

		if f.id == 0 {
			f.id = tx.PutInode(f.node)
		} else {
			tx.SetInode(f.id, f.node)
		}
		return nil
	})

	return nil
}

func (f *File) Readdirnames(n int) (list []string, err error) {
	var infos []os.FileInfo
	infos, err = f.Readdir(n)
	if err != nil {
		return nil, err
	}
	list = make([]string, len(infos))
	for i := range infos {
		list[i] = infos[i].Name()
	}

	return list, nil
}

func (f *File) Truncate(size int64) error {
	f.data = f.data[:int(size)]
	return nil
}

func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

type FileInfo struct {
	name string
	node *inode
}

func (f *FileInfo) Name() string {
	return f.name
}

func (f *FileInfo) Mode() os.FileMode {
	return f.node.Mode
}

func (f *FileInfo) IsDir() bool {
	return f.Mode()&os.ModeDir != 0
}

func (f *FileInfo) ModTime() time.Time {
	return f.node.Mtime
}

func (f *FileInfo) Size() int64 {
	return f.node.Size
}

func (f *FileInfo) Sys() interface{} {
	return f.node
}
