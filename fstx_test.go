package boltfs

import (
	"os"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func Test_newFsBucket(t *testing.T) {
	type args struct {
		tx         *bolt.Tx
		bucketpath string
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "global bucket",
			args: args{tx: nil, bucketpath: ""},
		},
		{
			name: "named bucket",
			args: args{tx: nil, bucketpath: "foo"},
		},
		{
			name: "nested buckets",
			args: args{tx: nil, bucketpath: "/bar/baz/bat"},
		},
	}

	// setup
	db, err := bolt.Open("test.db", 0644, nil)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		bucketpath := tt.args.bucketpath
		var want *fsBucket

		err = db.Update(func(tx *bolt.Tx) error {

			// create buckets
			err := bucketInit(tx, bucketpath)
			if err != nil {
				t.Error(err)
				return err
			}

			// open buckets
			b, err := openBucket(tx, bucketpath)
			if err != nil {
				t.Error(err)
				return err
			}

			// manually constructed reference fsBucket
			want = &fsBucket{
				state:    b.Bucket([]byte("state")),
				inodes:   b.Bucket([]byte("inodes")),
				data:     b.Bucket([]byte("data")),
				symlinks: b.Bucket([]byte("symlinks")),
			}

			// tests
			t.Logf("%q %q", tt.name, bucketpath)
			// b, err = openBucket(tx, bucketpath)
			// if err != nil {
			// 	t.Error(err)
			// 	return err
			// }

			got := newFsBucket(b)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("newFsBucket(, %q) = %v, want %v", bucketpath, got, want)
			}
			return nil
		})
	}
}

func Test_fsBucket_NextInode(t *testing.T) {
	tests := []struct {
		want    uint64
		save    bool
		wantErr bool
	}{
		{
			want:    1,
			save:    false,
			wantErr: false,
		},
		{
			want:    2,
			save:    true,
			wantErr: false,
		},
		{
			want:    3,
			save:    false,
			wantErr: false,
		},
	}
	testfile := "NextInode_test.db"
	// setup
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close() // Close the database before removing the file (required on Windows)
		err := os.Remove(testfile)
		if err != nil {
			t.Error(err)
		}
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		err := bucketInit(tx, "")
		if err != nil {
			return err
		}
		b, err := openBucket(tx, "")
		if err != nil {
			return err
		}
		f := newFsBucket(b)

		for _, tt := range tests {
			var ino uint64
			if tt.save {
				node := newInode(0)
				err = f.PutInode(0, node)
				ino = node.Ino
			} else {
				ino, err = f.NextInode()
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.NextInode() error = %v, wantErr %v", err, tt.wantErr)
				continue
			}
			if ino != tt.want {
				t.Errorf("fsBucket.NextInode() = %v, want %v", ino, tt.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}

}

func Test_fsBucket_InodeInit(t *testing.T) {
	testfile := "InodeInit_test.db"
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove(testfile)
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		err := bucketInit(tx, "")
		if err != nil {
			return err
		}
		b, err := openBucket(tx, "")
		if err != nil {
			return err
		}
		f := newFsBucket(b)

		// Test initialization
		err = f.InodeInit()
		if err != nil {
			t.Errorf("fsBucket.InodeInit() error = %v", err)
			return err
		}

		// Verify that inodes were created by checking NextSequence
		seq, err := f.NextInode()
		if err != nil {
			t.Errorf("NextInode() error = %v", err)
			return err
		}
		// After InodeInit, NextSequence should be at least 2
		// (inode 0 is nil, inode 1 is root/first real inode)
		if seq < 2 {
			t.Errorf("Expected NextInode >= 2 after init, got %d", seq)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func Test_fsBucket_LoadOrSet(t *testing.T) {

	type args struct {
		key   string
		value []byte
	}

	testfile := "LoadOrSet_test.db"
	// setup
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(testfile)

	err = db.Update(func(tx *bolt.Tx) error {

		// create buckets
		err := bucketInit(tx, "")
		if err != nil {
			t.Error(err)
			return err
		}

		// open buckets
		b, err := openBucket(tx, "")
		if err != nil {
			t.Error(err)
			return err
		}
		f := newFsBucket(b)
		tests := []struct {
			name    string
			f       *fsBucket
			args    args
			want    []byte
			wantErr bool
		}{
			{
				name:    "test1",
				f:       f,
				args:    args{key: "key1", value: []byte("value1")},
				want:    []byte("value1"),
				wantErr: bool(false),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				got, err := test.f.LoadOrSet(test.args.key, test.args.value)
				if (err != nil) != test.wantErr {
					t.Errorf("fsBucket.LoadOrSet() error = %v, wantErr %v", err, test.wantErr)
					return
				}
				if !reflect.DeepEqual(got, test.want) {
					t.Errorf("fsBucket.LoadOrSet() = %v, want %v", got, test.want)
				}
				return
			})

		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func Test_fsBucket_GetPutInode(t *testing.T) {
	testfile := "GetPutInode_test.db"
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove(testfile)
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		err := bucketInit(tx, "")
		if err != nil {
			return err
		}
		b, err := openBucket(tx, "")
		if err != nil {
			return err
		}
		f := newFsBucket(b)

		// Create a test inode
		testNode := newInode(0755)
		testNode.Size = 1234

		// Put the inode with ino=0 (will auto-generate)
		err = f.PutInode(0, testNode)
		if err != nil {
			t.Errorf("PutInode() error = %v", err)
			return err
		}

		// Get the inode back
		ino := testNode.Ino
		got, err := f.GetInode(ino)
		if err != nil {
			t.Errorf("GetInode() error = %v", err)
			return err
		}

		// Verify it matches
		if got.Size != testNode.Size {
			t.Errorf("GetInode() Size = %v, want %v", got.Size, testNode.Size)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func Test_fsBucket_GetPut(t *testing.T) {
	testfile := "GetPut_test.db"
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove(testfile)
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		err := bucketInit(tx, "")
		if err != nil {
			return err
		}
		b, err := openBucket(tx, "")
		if err != nil {
			return err
		}
		f := newFsBucket(b)

		// Test Put and Get
		testKey := "testkey"
		testData := []byte("test value")

		err = f.Put(testKey, testData)
		if err != nil {
			t.Errorf("Put() error = %v", err)
			return err
		}

		got, err := f.Get(testKey)
		if err != nil {
			t.Errorf("Get() error = %v", err)
			return err
		}

		if !reflect.DeepEqual(got, testData) {
			t.Errorf("Get() = %v, want %v", got, testData)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func Test_fsBucket_SymlinkReadlink(t *testing.T) {
	testfile := "SymlinkReadlink_test.db"
	db, err := bolt.Open(testfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.Remove(testfile)
	}()

	err = db.Update(func(tx *bolt.Tx) error {
		err := bucketInit(tx, "")
		if err != nil {
			return err
		}
		b, err := openBucket(tx, "")
		if err != nil {
			return err
		}
		f := newFsBucket(b)

		// Test Symlink and Readlink
		testIno := uint64(123)
		testPath := "/path/to/target"

		err = f.Symlink(testIno, testPath)
		if err != nil {
			t.Errorf("Symlink() error = %v", err)
			return err
		}

		got := f.Readlink(testIno)
		if got != testPath {
			t.Errorf("Readlink() = %v, want %v", got, testPath)
		}

		// Test reading non-existent symlink
		emptyPath := f.Readlink(999)
		if emptyPath != "" {
			t.Errorf("Readlink(999) = %v, want empty string", emptyPath)
		}

		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
