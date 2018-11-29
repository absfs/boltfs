package boltfs

import (
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
			want:    1,
			save:    true,
			wantErr: false,
		},
		{
			want:    2,
			save:    false,
			wantErr: false,
		},
	}

	// setup
	db, err := bolt.Open("test.db", 0644, nil)
	if err != nil {
		t.Fatal(err)
	}

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

			got, err := f.NextInode()
			node := newInode(0)
			if tt.save {
				err = f.PutInode(got, node)
				if err != nil {
					return err
				}
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.NextInode() error = %v, wantErr %v", err, tt.wantErr)
				return nil
			}
			if got != tt.want {
				t.Errorf("fsBucket.NextInode() = %v, want %v", got, tt.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func Test_fsBucket_InodeInit(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			if err := f.InodeInit(); (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.InodeInit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_fsBucket_LoadOrSet(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		key   string
		value []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			got, err := f.LoadOrSet(tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.LoadOrSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fsBucket.LoadOrSet() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fsBucket_PutInode(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		ino  uint64
		node *iNode
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			if err := f.PutInode(tt.args.ino, tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.PutInode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_fsBucket_GetInode(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *iNode
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			got, err := f.GetInode(tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.GetInode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fsBucket.GetInode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fsBucket_Get(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			got, err := f.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fsBucket.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fsBucket_Put(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		key  string
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			if err := f.Put(tt.args.key, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_fsBucket_Symlink(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		ino  uint64
		path string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			if err := f.Symlink(tt.args.ino, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.Symlink() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_fsBucket_Readlink(t *testing.T) {
	type fields struct {
		state    *bolt.Bucket
		inodes   *bolt.Bucket
		data     *bolt.Bucket
		symlinks *bolt.Bucket
	}
	type args struct {
		ino uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fsBucket{
				state:    tt.fields.state,
				inodes:   tt.fields.inodes,
				data:     tt.fields.data,
				symlinks: tt.fields.symlinks,
			}
			got, err := f.Readlink(tt.args.ino)
			if (err != nil) != tt.wantErr {
				t.Errorf("fsBucket.Readlink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("fsBucket.Readlink() = %v, want %v", got, tt.want)
			}
		})
	}
}
