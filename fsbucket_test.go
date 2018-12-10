package boltfs

import (
	"os"
	"reflect"
	"testing"

	bolt "github.com/coreos/bbolt"
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
	tests := []struct {
		name    string
		f       *fsBucket
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := test.f.InodeInit()
			if (err != nil) != test.wantErr {
				t.Errorf("fsBucket.InodeInit() error = %v, wantErr %v", err, test.wantErr)
			}
		})
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
	type args struct {
		ino  uint64
		node *iNode
	}
	tests := []struct {
		name    string
		f       *fsBucket
		args    args
		want    *iNode
		wantErr bool
	}{
		// TODO: Add test cases.
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := test.f.PutInode(test.args.ino, test.args.node)
			if err != nil {
				t.Error(err)
			}

			got, err := test.f.GetInode(test.args.ino)
			if (err != nil) != test.wantErr {
				t.Errorf("fsBucket.GetInode() error = %v, wantErr %v", err, test.wantErr)
				return
			}

			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("fsBucket.GetInode() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_fsBucket_GetPut(t *testing.T) {

	type args struct {
		key  string
		data []byte
	}

	tests := []struct {
		name    string
		f       *fsBucket
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := test.f.Put(test.args.key, test.args.data)
			if err != nil {
				t.Error(err)
			}

			got, err := test.f.Get(test.args.key)
			if (err != nil) != test.wantErr {
				t.Errorf("fsBucket.Put() error = %v, wantErr %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("fsBucket.Get() = %v, want %v", got, test.want)
			}
		})
	}
}

func Test_fsBucket_SymlinkReadlink(t *testing.T) {
	type args struct {
		ino  uint64
		path string
	}
	tests := []struct {
		name    string
		f       *fsBucket
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			err := test.f.Symlink(test.args.ino, test.args.path)
			if (err != nil) != test.wantErr {
				t.Errorf("fsBucket.Symlink() error = %v, wantErr %v", err, test.wantErr)
			}

			got := test.f.Readlink(test.args.ino)
			if got != test.want {
				t.Errorf("fsBucket.Readlink() = %v, want %v", got, test.want)
			}
		})
	}
}
