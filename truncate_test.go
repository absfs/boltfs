package boltfs

import (
	"os"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// TestOpenWithTruncateUpdatesInode verifies that opening a file with O_TRUNC
// properly updates the inode's size and modification time
func TestOpenWithTruncateUpdatesInode(t *testing.T) {
	// Create a temporary database
	tmpfile := "/tmp/test_truncate.db"
	defer os.Remove(tmpfile)

	db, err := bolt.Open(tmpfile, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}

	fs, err := NewFS(db, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file with some content
	f, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	content := []byte("Hello, World! This is some test content.")
	n, err := f.Write(content)
	if err != nil || n != len(content) {
		t.Fatalf("Failed to write content: %v", err)
	}
	f.Close()

	// Get initial file info
	info1, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info1.Size() != int64(len(content)) {
		t.Errorf("Initial size incorrect: got %d, want %d", info1.Size(), len(content))
	}

	initialMtime := info1.ModTime()

	// Wait a bit to ensure time difference is detectable
	time.Sleep(10 * time.Millisecond)

	// Open the file with O_TRUNC
	f, err = fs.OpenFile("/test.txt", os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("Failed to open file with O_TRUNC: %v", err)
	}
	f.Close()

	// Get file info after truncate
	info2, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Failed to stat file after truncate: %v", err)
	}

	// Verify size is 0
	if info2.Size() != 0 {
		t.Errorf("Size after O_TRUNC: got %d, want 0", info2.Size())
	}

	// Verify modification time was updated
	newMtime := info2.ModTime()
	if !newMtime.After(initialMtime) {
		t.Errorf("Modification time not updated: initial %v, after truncate %v", initialMtime, newMtime)
	}

	// Verify the file data is actually empty
	f, err = fs.Open("/test.txt")
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 100)
	n, _ = f.Read(buf)
	if n != 0 {
		t.Errorf("File should be empty after truncate, but read %d bytes", n)
	}
}
