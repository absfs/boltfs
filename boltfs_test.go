package boltfs

import (
	"os"
	"testing"

	"github.com/absfs/absfs"
)

func TestBoltFS(t *testing.T) {
	dbpath := "testing_BoltFS.db"
	boltfs, err := New(dbpath)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		boltfs.Close()
		os.RemoveAll(dbpath)
	}()

	var filer absfs.Filer

	filer = boltfs

	_ = filer
}
