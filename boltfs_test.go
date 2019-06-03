package boltfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/absfs/absfs"
	bolt "github.com/etcd-io/bbolt"
)

func BeforeEach(t *testing.T) *bolt.DB {
	dbpath := "testing_BoltFS.db"

	// remove any previous test state
	os.RemoveAll(dbpath)

	// setup
	// boltfs, err := NewFS(dbpath, "")
	db, err := bolt.Open(dbpath, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func TestBoltFS(t *testing.T) {
	db := BeforeEach(t)
	defer db.Close()

	var fs absfs.SymlinkFileSystem
	var err error

	fs, err = NewFS(db, "")
	if err != nil {
		t.Error(err)
	}
	_ = fs
}

func TestFileSystem(t *testing.T) {

	dbpath := "testing_BoltFS.db"

	// remove any previous test state
	os.RemoveAll(dbpath)

	// setup
	// boltfs, err := NewFS(dbpath, "")
	db, err := bolt.Open(dbpath, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}

	bfs, err := NewFS(db, "")
	if err != nil {
		t.Fatal(err)
	}

	// test interface compatibility
	var fs absfs.FileSystem
	fs = bfs

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
	if bfs == nil {
		t.Fatal("bfs == nil")
	}
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
	if bfs == nil {
		t.Fatal("bfs == nil")
	}
	t.Run("state", func(t *testing.T) {
		mode := bfs.Umask()
		if mode != 0755 {
			t.Errorf("incorrect default umask %o", mode)
		}

		bfs.SetUmask(0700)

		mode = bfs.Umask()
		if mode != 0700 {
			t.Error("incorrect updated umask")
		}

		tempdir := bfs.TempDir()
		if tempdir != "/tmp" {
			t.Error("incorrect default temp dir")
		}

		bfs.SetTempdir("/foo/bar")
		tempdir = bfs.TempDir()
		if tempdir != "/foo/bar" {
			t.Error("incorrect updated temp dir")
		}
		if bfs == nil {
			t.Fatal("bfs == nil")
		}
		// Close
		err = bfs.Close()
		if err != nil {
			t.Error(err)
		}

		err = db.Close()
		if err != nil {
			t.Error(err)
		}
		// Re-open
		db, err := bolt.Open(dbpath, 0644, nil)
		if err != nil {
			t.Fatal(err)
		}
		bfs, err = NewFS(db, "")
		if err != nil {
			t.Fatal(err)
		}
		fs = bfs
		if bfs == nil {
			t.Fatal("bfs == nil")
		}
		mode = bfs.Umask()
		if mode != 0700 {
			t.Error("incorrect updated umask")
		}

		tempdir = bfs.TempDir()
		if tempdir != "/foo/bar" {
			t.Error("incorrect updated temp dir")
		}

	})
	if bfs == nil {
		t.Fatal("bfs == nil")
	}
	t.Run("separators", func(t *testing.T) {

		if bfs.Separator() != '/' {
			t.Errorf("wrong path separator %q", bfs.Separator())
		}

		if bfs.ListSeparator() != ':' {
			t.Errorf("wrong list separator %q", bfs.ListSeparator())
		}

	})
	if bfs == nil {
		t.Fatal("bfs == nil")
	}
	t.Run("directory", func(t *testing.T) {

		err := fs.Mkdir("foo", 0755)
		if err != nil {
			f, e := fs.Open("/")
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()

			list, e := f.Readdirnames(-1)
			if e != nil {
				t.Fatal(e)
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

				f, err := bfs.Open(test.Path)
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

		err := bfs.Rename("/"+testfile, testfile)
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

		err = bfs.Copy("/foo/bar/baz/"+testfile, "/foo/"+testfile)
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
		"/foo/bar/baz/test_file.txt",
		"/foo/test_file.txt",
		"/truncate_file.tmp",
	}
	i := 0
	t.Run("walk", func(t *testing.T) {
		err := bfs.Walk("/", func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if i < len(walkpaths) {
				if walkpaths[i] != path {
					t.Errorf("wrong path: %s, %s", walkpaths[i], path)
				}
			}
			i++
			t.Logf("%s\n", path)
			return nil
		})
		if err != nil {
			t.Error(err)
		}

		if i != len(walkpaths) {
			t.Errorf("wrong count %d, %d", i, len(walkpaths))
		}
	})

	// test remove
	err = fs.Remove("/truncate_file.tmp")
	if err != nil {
		t.Error(err)
	}

	f, err := fs.Open("/")
	if err != nil {
		t.Error(err)
	}

	names, err := f.Readdirnames(-1)
	if err != nil {
		t.Error(err)
	}
	f.Close()
	for _, name := range names {
		if name == "truncate_file.tmp" {
			t.Errorf("file not removed. %q, %q", name, "truncate_file.tmp")
		}
	}

	err = fs.RemoveAll("/")
	if err != nil {
		t.Error(err)
	}

	err = bfs.Close()
	if err != nil {
		t.Error(err)
	}
	// cleanup
	err = os.RemoveAll(dbpath)
	if err != nil {
		t.Error(err)
	}

}

func TestSymlinks(t *testing.T) {
	dbpath := "testingSymlinks.db"

	// remove any previous test state
	os.RemoveAll(dbpath)

	db, err := bolt.Open(dbpath, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	// setup
	boltfs, err := NewFS(db, "")
	if err != nil {
		t.Fatal(err)
	}

	// test interface compatibility
	var fs absfs.SymlinkFileSystem
	fs = boltfs

	type Test struct {
		Label    string
		Path     string
		Readlink string
		Error    string
	}

	testdata := []Test{
		{
			Label:    "abs path",
			Path:     "/foo/bar/baz",
			Readlink: "/foo/baz",
			Error:    "",
		},
		{
			Label:    "rel path",
			Path:     "/foo/bar/bat",
			Readlink: "../../bat",
			Error:    "",
		},
		{
			Label:    "same dir",
			Path:     "/bat",
			Readlink: "/foo",
			Error:    "",
		},
		{
			Label:    "broken",
			Path:     "/broken",
			Readlink: "/nil",
			Error:    "file does not exist",
		},
		{
			Label:    "circular absolute",
			Path:     "/circular/one/two/three",
			Readlink: "/circular/one",
			Error:    "",
		},
		{
			Label:    "circular relative",
			Path:     "/circular/one/two/four",
			Readlink: "../..",
			Error:    "",
		},
	}

	t.Run("symlinks", func(t *testing.T) {
		//Path to test map to make testing a little easier
		linkmap := make(map[string]Test)

		// build directory structure, with all symlink targets
		for _, test := range testdata {
			linkmap[test.Path] = test

			// folders in which files will be created
			fs.MkdirAll(filepath.Dir(test.Path), 0700)

			link := test.Readlink
			if !filepath.IsAbs(test.Readlink) {
				link = filepath.Join(test.Path, test.Readlink)
			}

			// folders that symlinks will link to
			fs.MkdirAll(link, 0700)

		}

		// make symlinks
		for _, test := range testdata {

			// The result of "fs.Readlink" is esssentally the same as the `source`
			// value in a copy, so `test.Readlink` is the 'source', and
			// `test.Path` is a symbolic link to it (a.k.a the 'target')
			err := boltfs.Symlink(test.Readlink, test.Path)
			if err != nil {
				t.Fatalf("should not error: %s", err)
			}
		}

		// remove the broken link target to break the link
		err := fs.Remove("/nil")
		if err != nil {
			t.Error(err)
		}

		// `count` and `limit` are to prevent infinite walks if implementation fails
		// to stop on symlinks as defined by `filepath.Walk`.
		var count, limit int
		limit = 100

		err = boltfs.Walk("/", func(path string, info os.FileInfo, err error) error {
			t.Log(path)
			link := ""
			if info.Mode()&os.ModeSymlink != 0 {
				link, err = fs.Readlink(path)
				if err != nil {
					return err
				}
			}
			if linkmap[path].Readlink != link {
				t.Errorf("expected symlink %q -> %q", path, linkmap[path])
			}

			stat, err := fs.Stat(path)
			if len(linkmap[path].Error) == 0 {
				if err != nil {
					t.Errorf("unexpected error %s", err)
				}
			} else {
				if err == nil {
					t.Errorf("expected error missing %s", linkmap[path].Error)
				}
			}

			if stat != nil {
				t.Logf(" Stat: %s %q %s", stat.Mode(), stat.Name(), link)
			} else {
				t.Logf("Stat: <nil>")
			}

			lstat, err := fs.Lstat(path)
			if err != nil {
				t.Errorf("fs.Lstat error=%s", err)
			}
			if lstat != nil {
				t.Logf("Lstat: %s %q %s\n", lstat.Mode(), lstat.Name(), link)
			} else {
				t.Logf("Lstat: <nil>\n")
			}
			count++
			if count > limit {
				return errors.New("too deep")
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
	})

	err = fs.RemoveAll("/")
	if err != nil {
		t.Error(err)
	}

	err = boltfs.Close()
	if err != nil {
		t.Error(err)
	}
	// cleanup
	err = os.RemoveAll(dbpath)
	if err != nil {
		t.Error(err)
	}

}
