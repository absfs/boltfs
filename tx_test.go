package boltfs

import (
	"bytes"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"

	bolt "github.com/coreos/bbolt"
)

func TestTx(t *testing.T) {
	dbpath := "testing_TestTx.db"
	db, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbpath)
	}()

	err = db.Update(func(btx *bolt.Tx) error {
		for _, name := range []string{"inodes", "dirs", "data", "labels"} {
			_, err := btx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}

		tx := &Tx{tx: btx}
		_ = tx

		rootId := tx.PutInode(newDirNode(0755))
		tx.SetLabel("latest", rootId)

		node := newNode(0644)
		id := tx.PutInode(node)

		dirs := tx.GetDirs(rootId)
		dirs = append(dirs, Entry{0, id, "foo.txt"})
		tx.SetDirs(rootId, dirs)

		id = tx.Mkdir("/foo", 0700)
		if id == 0 {
			return tx.err
		}
		id = tx.Mkdir("/foo/bar", 0700)
		if id == 0 {
			return tx.err
		}
		id = tx.Mkdir("/foo/bar/bat", 0700)
		if id == 0 {
			return tx.err
		}
		return tx.err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(btx *bolt.Tx) error {
		tx := &Tx{tx: btx}

		err := tx.Chdir("/foo/bar")
		if err != nil {
			return err
		}
		id, err := tx.EvalPath("")
		node := tx.GetInode(id)
		tm := time.Now().Add(-2 * time.Hour)
		node.SetAccess(tm)
		node.SetModified(tm.Add(time.Minute))
		dirs := tx.GetDirs(id)
		if dirs.String() != `Dir{{"bat": 5}}` {
			t.Fatalf("unexpected dirs string: %q", dirs.String())
		}
		err = tx.Chdir("bat")
		if err != nil {
			return err
		}

		return tx.err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Update(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		f, err := tx.Open("/foo/bar/bat/testFile.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatal(err)
			return err
		}
		defer f.Close()

		body := []byte("Hello, world!\n")
		n, err := f.Write(body)
		if err != nil {
			return err
		}
		if len(body) != n {
			t.Fatalf("not all data written %d, %d", len(body), n)
		}
		return tx.err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(btx *bolt.Tx) error {
		tx := NewTx(btx)
		f, err := tx.Open("/foo/bar/bat/testFile.txt", os.O_RDONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		expected := []byte("Hello, world!\n")
		data := make([]byte, len(expected)*2)

		n, err := f.Read(data)
		if err != nil {
			return err
		}
		data = data[:n]

		if len(data) == 0 {
			t.Error("no data read")
		}
		if bytes.Compare(expected, data) != 0 {
			t.Errorf("read does not match write: %q != %q", string(expected), string(data))
		}

		return tx.err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxTraversal(t *testing.T) {
	dbpath := "testing_TestTx.db"
	db, err := bolt.Open(dbpath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.Close()
		os.RemoveAll(dbpath)
	}()

	err = db.Update(func(btx *bolt.Tx) error {
		for _, name := range []string{"inodes", "dirs", "data", "labels"} {
			_, err := btx.CreateBucketIfNotExists([]byte(name))
			if err != nil {
				return err
			}
		}

		tx := NewTx(btx)
		id := tx.GetLabel("latest")
		if id == 0 {
			id = tx.PutInode(newDirNode(0755))
			tx.SetLabel("latest", id)
		}
		id = tx.Mkdir("/F", 0755)
		id = tx.Mkdir("/F/B", 0755)
		id = tx.Mkdir("/F/B/A", 0755)
		id = tx.Mkdir("/F/B/D", 0755)
		id = tx.Mkdir("/F/B/D/C", 0755)
		id = tx.Mkdir("/F/B/D/E", 0755)
		id = tx.Mkdir("/F/G", 0755)
		id = tx.Mkdir("/F/G/I", 0755)
		id = tx.Mkdir("/F/G/I/H", 0755)

		var values []string
		err = tx.PostOrder("/F", func(name string, id uint64, err error) error {
			values = append(values, name)
			return nil
		})
		expected := []string{"A", "C", "E", "D", "B", "H", "I", "G", "F"}
		for i := range expected {
			if values[i] != expected[i] {
				log.Errorf("%d doesn't match: %s, %s", values[i], expected[i])
			}
		}
		return err

	})
	if err != nil {
		t.Fatal(err)
	}

}
