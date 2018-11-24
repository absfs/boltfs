package boltfs

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/absfs/absfs"
)

func TestBoltFS(t *testing.T) {

	dbpath := "testing_BoltFS.db"

	// remove any previous test state
	os.RemoveAll(dbpath)

	// setup
	boltfs, err := NewFS(dbpath)

	if err != nil {
		t.Fatal(err)
	}

	// test interface compatibility
	var fs absfs.FileSystem
	fs = boltfs

	message := "Hello, world!\n"
	testfile := "test_file.txt"
	t.Run("create file", func(t *testing.T) {

		f, err := fs.Create(testfile)
		if err != nil {
			t.Error(err)
			return
		}

		err = f.Close()
		if err != nil {
			t.Error(err)
			return
		}

	})

	t.Run("read write", func(t *testing.T) {

		f, err := fs.OpenFile(testfile, os.O_RDWR, 0666)
		if err != nil {
			t.Error(err)
			return
		}

		t.Run("write", func(t *testing.T) {
			n, err := f.Write([]byte(message))
			if err != nil {
				t.Fatal(err)
			}
			if n != 14 {
				t.Fatal("wrong write count")
			}

		})

		err = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		// })

		f, err = fs.Open(testfile)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				t.Error(err)
			}
		}()

		t.Run("read", func(t *testing.T) {

			data := make([]byte, 512)
			n, err := f.Read(data)
			if err != nil {
				t.Fatal(err)
			}
			if n != 14 {
				t.Fatal("wrong read count")
			}

			data = data[:n]
			if string(data) != string(message) {
				t.Fatalf("wrong message read %q %q", string(data), string(message))
			}
		})

	})

	t.Run("state", func(t *testing.T) {
		mode := boltfs.Umask()
		if mode != 0777 {
			t.Error("incorrect default umask")
		}

		boltfs.SetUmask(0700)

		mode = boltfs.Umask()
		if mode != 0700 {
			t.Error("incorrect updated umask")
		}

		tempdir := boltfs.TempDir()
		if tempdir != "/tmp" {
			t.Error("incorrect default temp dir")
		}

		boltfs.SetTempdir("/foo/bar")
		tempdir = boltfs.TempDir()
		if tempdir != "/foo/bar" {
			t.Error("incorrect updated temp dir")
		}

		// Close
		err = boltfs.Close()
		if err != nil {
			t.Error(err)
		}

		// Re-open
		boltfs, err = NewFS(dbpath)
		if err != nil {
			t.Fatal(err)
		}
		fs = boltfs

		mode = boltfs.Umask()
		if mode != 0700 {
			t.Error("incorrect updated umask")
		}

		tempdir = boltfs.TempDir()
		if tempdir != "/foo/bar" {
			t.Error("incorrect updated temp dir")
		}

	})

	t.Run("separators", func(t *testing.T) {

		if boltfs.Separator() != '/' {
			t.Errorf("wrong path separator %q", boltfs.Separator())
		}

		if boltfs.ListSeparator() != ':' {
			t.Errorf("wrong list separator %q", boltfs.ListSeparator())
		}

	})

	t.Run("directory", func(t *testing.T) {

		err := fs.Mkdir("foo", 0755)
		if err != nil {
			f, e := fs.Open("/")
			if e != nil {
				panic(e)
			}
			defer f.Close()

			list, e := f.Readdirnames(-1)
			if e != nil {
				panic(e)
			}

			t.Logf("files %s", list)
			t.Error(err)
		}

		err = fs.MkdirAll("foo/bar/baz", 0755)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("working directory", func(t *testing.T) {
		cwd, err := fs.Getwd()
		if err != nil {
			t.Error(err)
		}
		if cwd != "/" {
			t.Errorf("wrong working directory %q", cwd)
		}

		err = fs.Chdir("foo/bar/baz")
		if err != nil {
			t.Error(err)
		}

		cwd, err = fs.Getwd()
		if err != nil {
			t.Error(err)
		}
		if cwd != "/foo/bar/baz" {
			t.Errorf("wrong working directory %q", cwd)
		}

		// negative case
		err = fs.Chdir("/foo/baz/bar")
		if err == nil {
			t.Error("expected error")
		}
		cwd, err = fs.Getwd()
		if err != nil {
			t.Error(err)
		}
		if cwd != "/foo/bar/baz" {
			t.Errorf("wrong working directory %q", cwd)
		}

	})

	t.Run("move copy", func(t *testing.T) {

		type testStruct struct {
			Path     string
			Expected []string
		}

		testloop := func(tests []testStruct) {
			for _, test := range tests {

				f, err := boltfs.Open(test.Path)
				if err != nil {
					t.Errorf("%q %s", test.Path, err)
					return
				}
				infos, err := f.Readdir(-1)
				f.Close()
				if err != nil {
					if err != io.EOF {
						t.Error(err)
					}
					return
				}

				expected := test.Expected
				if len(expected) != len(infos) {
					t.Errorf("incorrect item count, expected %d, got %d", len(expected), len(infos))
				}
				for i, info := range infos {
					if i >= len(expected) {
						continue
					}
					if expected[i] != info.Name() {
						t.Errorf("wrong filename %d: %q", i, info.Name())
					}
				}

			}
		}

		// `Move` should change the directory structure.
		// These values describe the file disposition before the move.
		testsBefore := []testStruct{
			{
				Path:     "/",
				Expected: []string{"foo", testfile},
			}, {
				Path:     "/foo/bar/baz",
				Expected: []string{},
			},
		}
		// run the tests described inline above
		testloop(testsBefore)

		err := boltfs.Move("/"+testfile, testfile)
		if err != nil {
			t.Error(err)
			return
		}

		// These values describe the file disposition after the move.
		testsAfter := []testStruct{
			{
				Path:     "/",
				Expected: []string{"foo"},
			}, {
				Path:     "/foo/bar/baz",
				Expected: []string{testfile},
			},
		}
		// run the tests described inline above
		testloop(testsAfter)

		// We'll start with the current layout as the `testsBefore` values
		testsBefore = []testStruct{
			{
				Path:     "/foo",
				Expected: []string{"bar"},
			}, {
				Path:     "/foo/bar/baz",
				Expected: []string{testfile},
			},
		}

		testloop(testsBefore)

		err = boltfs.Copy("/foo/bar/baz/"+testfile, "/foo/"+testfile)
		if err != nil {
			t.Error(err)
			return
		}

		testsAfter = []testStruct{
			{
				Path:     "/foo",
				Expected: []string{"bar", testfile},
			}, {
				Path:     "/foo/bar/baz",
				Expected: []string{testfile},
			},
		}
		testloop(testsAfter)

		// func (fs *FileSystem) Copy(source, destination string) error
	})

	t.Run("stat", func(t *testing.T) {

		info, err := fs.Stat("/foo/" + testfile)
		if err != nil {
			t.Error(err)
			return
		}

		if info.Name() != testfile {
			t.Error("wrong file name")
		}

		if info.Size() != 14 {
			t.Error("wrong size")
		}

		if info.Mode() != 0777 {
			t.Errorf("wrong mode %s", info.Mode())
		}

		if time.Since(info.ModTime()) > time.Second {
			t.Error("wrong modifiion time")
		}

		if info.IsDir() {
			t.Error("wrong directory flag")
		}

	})

	t.Run("metadata", func(t *testing.T) {
		now := time.Now()
		atime := now
		mtime := now

		name := "/foo/" + testfile

		beforeInfo, err := fs.Stat(name)
		if err != nil {
			t.Error(err)
			return
		}

		err = fs.Chtimes(name, atime, mtime)
		if err != nil {
			t.Error(err)
		}

		err = fs.Chown(name, 500, 100)
		if err != nil {
			t.Error(err)
		}

		err = fs.Chmod(name, 0700)
		if err != nil {
			t.Error(err)
		}

		afterInfo, err := fs.Stat(name)
		if err != nil {
			t.Error(err)
		}

		_, _ = beforeInfo, afterInfo
		if beforeInfo.Name() != afterInfo.Name() {
			t.Errorf("names donot match %q, %q", beforeInfo.Name(), afterInfo.Name())
		}
		if beforeInfo.Mode() == afterInfo.Mode() {
			t.Errorf("modes should not agree %s, %s", beforeInfo.Mode(), afterInfo.Mode())
		}
		if afterInfo.Mode() != 0700 {
			t.Errorf("incorrect mode%s", afterInfo.Mode())
		}

		if beforeInfo.ModTime().Equal(afterInfo.ModTime()) {
			t.Errorf("modification times should be different %s, %s", beforeInfo.ModTime(), afterInfo.ModTime())
		}

	})

	t.Run("truncate", func(t *testing.T) {
		name := "/truncate_file.tmp"
		_, err := fs.Stat(name)
		if err == nil {
			t.Errorf("expected error")
			return
		}

		for _, size := range []int64{0, 10, 100, 23820, 0, 23589} {

			err = fs.Truncate(name, size)
			if err != nil {
				t.Error(err)
				return
			}

			info, err := fs.Stat(name)
			if err != nil {
				t.Error(err)
				return
			}

			if info.Size() != size {
				t.Errorf("incorrect size %d, %d", info.Size(), size)
			}

		}

	})

	walkpaths := []string{
		"/",
		"/foo",
		"/foo/bar",
		"/foo/bar/baz",
		"/foo/test_file.txt",
		"/truncate_file.tmp",
	}
	i := 0
	t.Run("walk", func(t *testing.T) {
		err := boltfs.Walk("/", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if walkpaths[i] != path {
				t.Errorf("wrong path: %s, %s", walkpaths[i], path)
			}
			i++
			// fmt.Printf("%s\n", path)
			return nil
		})
		if err != nil {
			t.Error(err)
		}

		if i != len(walkpaths) {
			t.Errorf("wrong count %d, %d", i, len(walkpaths))
		}
	})

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

// func (fs *FileSystem) Lstat(name string) (os.FileInfo, error)
// func (fs *FileSystem) Lchown(name string, uid, gid int) error
// func (fs *FileSystem) Readlink(name string) (string, error)
// func (fs *FileSystem) Symlink(oldname, newname string) error

// func (fs *FileSystem) FastWalk(name string, fn func(string, os.FileMode) error) error
