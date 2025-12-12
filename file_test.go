package boltfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/absfs/absfs"
	bolt "go.etcd.io/bbolt"
)

func TestFile(t *testing.T) {
	dbpath := "testing_BoltFS.db"

	// remove any previous test state
	os.RemoveAll(dbpath)

	db, err := bolt.Open(dbpath, 0644, nil)
	// setup
	boltfs, err := NewFS(db, "")
	if err != nil {
		t.Fatal(err)
	}

	// test interface compatibility
	var fs absfs.FileSystem
	fs = boltfs

	message := "Hello, world!\n"
	testfile := "file test"

	test := "open close"
	t.Run(test, func(t *testing.T) {
		filename := camel(strings.Join([]string{testfile, test, ".txt"}, " "))

		f, err := fs.Create(filename)
		if err != nil {
			t.Error(err)
			return
		}
		if f.Name() != filename {
			t.Errorf("wrong Name() %s,%s", f.Name(), filename)
		}

		err = f.Close()
		if err != nil {
			t.Error(err)
		}

	})

	test = "read write"
	t.Run(test, func(t *testing.T) {

		filename := camel(strings.Join([]string{testfile, test, ".%04d.txt"}, " "))
		files := []string{fmt.Sprintf(filename, 1), fmt.Sprintf(filename, 2)}
		f, err := fs.Create(files[0])
		if err != nil {
			t.Error(err)
			return
		}

		n, err := f.Write([]byte(message))
		if err != nil {
			t.Error(err)
		}
		err = f.Close()
		if err != nil {
			t.Error(err)
		}

		if n != len(message) {
			t.Errorf("wrong length for %q %d, %d", files[0], n, len(message))
		}

		f, err = fs.Create(files[1])
		if err != nil {
			t.Error(err)
			return
		}

		n, err = f.WriteString(message)
		if err != nil {
			t.Error(err)
		}

		err = f.Close()
		if err != nil {
			t.Error(err)
		}

		if n != len(message) {
			t.Errorf("wrong length for %q %d, %d", files[1], n, len(message))
		}

		for _, filename := range files {
			f, err := fs.Open(filename)
			if err != nil {
				t.Error(err)
				continue
			}

			data := make([]byte, 512)
			n, err := f.Read(data)
			if err != nil {
				t.Error(err)
			}

			data = data[:n]
			if string(data) != message {
				t.Errorf("file contents do not match %q, %q", string(data), message)
			}

			err = f.Close()
			if err != nil {
				t.Error(err)
			}
		}

	})

	test = "seek read write at"
	t.Run(test, func(t *testing.T) {
		filename := camel(strings.Join([]string{testfile, test, ".txt"}, " "))

		f, err := fs.Create(filename)
		if err != nil {
			t.Error(err)
		}
		data, err := ioutil.ReadFile("file_test.go")
		if err != nil {
			t.Error(err)
		}

		n, err := f.Write(data)
		if err != nil {
			t.Error(err)
		}
		if n != len(data) {
			t.Errorf("wrong amount of data written %d, %d", n, len(data))
		}
		q := n / 4

		err = f.Close()
		if err != nil {
			t.Error(err)
		}

		// t.Logf("data size: %d, %d", n, len(data))
		tests := []struct {
			Offset int
			Start  int
			Size   int
			Whence int
		}{
			// all, from the beginning
			{
				Offset: 0,
				Start:  0,
				Size:   len(data),
				Whence: io.SeekStart,
			},

			// 2nd quarter, from the beginning
			{
				Offset: q,
				Start:  q,
				Size:   q,
				Whence: io.SeekStart,
			},

			// 3rd quarter, from current
			{
				Offset: 0,
				Start:  n / 2,
				Size:   q,
				Whence: io.SeekCurrent,
			},

			// 4th quarter, from end
			{
				Offset: -q,
				Start:  n - q,
				Size:   q,
				Whence: io.SeekEnd,
			},
		}

		for i, test := range tests {

			f, err := fs.OpenFile(filename, absfs.O_RDWR, 0666)
			if err != nil {
				t.Error(err)
				continue
			}

			// test Seek
			offset, err := f.Seek(int64(test.Start), io.SeekStart)
			if err != nil {
				t.Error(err)
			}
			if offset != int64(test.Start) {
				t.Error(err)
			}
			offset, err = f.Seek(int64(test.Offset), test.Whence)
			if err != nil {
				t.Error(err)
			}
			d := make([]byte, len(data))
			n, err := f.Read(d)
			if err != nil {
				t.Error(err)
			}
			d = d[:n]
			if bytes.Compare(d, data[test.Start:]) != 0 {
				t.Errorf("%d: data does not compare", i)
			}

			// test ReadAt
			d = make([]byte, len(data))
			n, err = f.ReadAt(d, int64(test.Start))
			if err != nil {
				t.Error(err)
			}
			d = d[:n]
			if bytes.Compare(d, data[test.Start:]) != 0 {
				t.Errorf("%d: data does not compare", i)
			}

			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				t.Error(err)
			}
			// test WriteAt
			_, err = f.WriteAt([]byte(message), int64(test.Start))

			copy(data[test.Start:], []byte(message))

			w := new(bytes.Buffer)
			_, err = f.Seek(0, io.SeekStart)
			if err != nil {
				t.Error(err)
			}

			offset, err = io.Copy(w, f)
			if err != nil {
				t.Error(err)
			}

			d = w.Bytes()

			if bytes.Compare(d, data) != 0 {
				t.Errorf("%d: write data does not compare", i)
			}

			err = f.Close()
			if err != nil {
				t.Error(err)
			}
		}

	})

	// func (f *File) Stat() (os.FileInfo, error)
	// func (f *File) Sync() error

	test = "readdir readdirnames"
	t.Run(test, func(t *testing.T) {
		f, err := fs.Open("/")
		if err != nil {
			t.Error(err)
		}

		infos, err := f.Readdir(-1)
		if err != nil {
			t.Error(err)
		}

		err = f.Close()
		if err != nil {
			t.Error(err)
		}

		info, err := os.Stat("file_test.go")
		if err != nil {
			t.Error(err)
		}
		size := info.Size()

		expected := []struct {
			Mode os.FileMode
			Size int64
			Name string
		}{
			{
				Mode: 0777,
				Size: 0,
				Name: "fileTestOpenClose.txt",
			},
			{
				Mode: 0777,
				Size: int64(len(message)),
				Name: "fileTestReadWrite.0001.txt",
			},
			{
				Mode: 0777,
				Size: int64(len(message)),
				Name: "fileTestReadWrite.0002.txt",
			},
			{
				Mode: 0777,
				Size: size,
				Name: "fileTestSeekReadWriteAt.txt",
			},
		}
		for i, info := range infos {
			if expected[i].Mode != info.Mode() ||
				expected[i].Size != info.Size() ||
				expected[i].Name != info.Name() ||
				time.Since(info.ModTime()) < time.Microsecond*100 {
				t.Logf("%d: %s % 6d %s %s\n", i, info.Mode(), info.Size(), info.ModTime(), info.Name())
				t.Logf("%d: %s % 6d %s %s\n", i, expected[i].Mode, expected[i].Size, time.Since(info.ModTime()), expected[i].Name)
				t.Error("Readdir os.FileInfo incorrect result")
			}
		}

		f, err = fs.Open("/")
		if err != nil {
			t.Error(err)
		}

		names, err := f.Readdirnames(-1)
		if err != nil {
			t.Error(err)
		}

		err = f.Close()
		if err != nil {
			t.Error(err)
		}

		for i, info := range expected {
			if names[i] != info.Name {
				t.Errorf("%d: readdirnames error, %q, %q", i, names[i], info.Name)
			}
		}
	})

	// removeAll test
	err = fs.RemoveAll("/")
	if err != nil {
		t.Error(err)
	}

	f, err := fs.Open("/")
	if err != nil {
		t.Error(err)
	}

	names, err := f.Readdirnames(-1)
	// When n <= 0, Readdirnames returns nil error even for empty directories
	// (this matches Go's standard library behavior for os.File)
	if err != nil {
		t.Errorf("Readdirnames(-1) should return nil error for empty dir, got: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("list should be empty %d", len(names))
	}

	err = f.Close()
	if err != nil {
		t.Error(err)
	}

	// cleanup
	err = boltfs.Close()
	if err != nil {
		t.Error(err)
	}

	err = os.RemoveAll(dbpath)
	if err != nil {
		t.Error(err)
	}

}

func camel(name string) string {
	var out []rune
	for _, w := range strings.Split(name, " ") {
		runes := []rune(w)
		runes[0] = unicode.ToUpper(runes[0])
		out = append(out, runes...)
	}
	out[0] = unicode.ToLower(out[0])
	return string(out)
}
