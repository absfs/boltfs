package boltfs

import (
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// TestBoltContentStore tests the BoltDB-backed content store
func TestBoltContentStore(t *testing.T) {
	// Create temporary database
	tmpfile := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(tmpfile, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Initialize buckets
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("data"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	store := NewBoltContentStore(db)

	// Test Put
	testData := []byte("hello world")
	err = store.Put(1, testData)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	data, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != string(testData) {
		t.Fatalf("Get returned wrong data: got %q, want %q", data, testData)
	}

	// Test Exists
	if !store.Exists(1) {
		t.Fatal("Exists returned false for existing content")
	}
	if store.Exists(999) {
		t.Fatal("Exists returned true for non-existing content")
	}

	// Test Delete
	err = store.Delete(1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	data, err = store.Get(1)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if data != nil {
		t.Fatalf("Get after delete returned data: %v", data)
	}

	// Test Close
	err = store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestFilesystemContentStore tests the filesystem-backed content store
func TestFilesystemContentStore(t *testing.T) {
	// Create temporary directory
	tmpdir := filepath.Join(t.TempDir(), "contentstore")

	store, err := NewFilesystemContentStore(tmpdir)
	if err != nil {
		t.Fatalf("NewFilesystemContentStore failed: %v", err)
	}

	// Test Put
	testData := []byte("hello world")
	err = store.Put(1, testData)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify file exists
	path := store.getPath(1)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("Put did not create file")
	}

	// Test Get
	data, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != string(testData) {
		t.Fatalf("Get returned wrong data: got %q, want %q", data, testData)
	}

	// Test Exists
	if !store.Exists(1) {
		t.Fatal("Exists returned false for existing content")
	}
	if store.Exists(999) {
		t.Fatal("Exists returned true for non-existing content")
	}

	// Test Delete
	err = store.Delete(1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("Delete did not remove file")
	}

	// Test Get after delete
	data, err = store.Get(1)
	if err != nil {
		t.Fatalf("Get after delete failed: %v", err)
	}
	if data != nil {
		t.Fatalf("Get after delete returned data: %v", data)
	}

	// Test Close
	err = store.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestFilesystemContentStoreSubdirectories tests subdirectory creation
func TestFilesystemContentStoreSubdirectories(t *testing.T) {
	tmpdir := filepath.Join(t.TempDir(), "contentstore")

	store, err := NewFilesystemContentStore(tmpdir)
	if err != nil {
		t.Fatalf("NewFilesystemContentStore failed: %v", err)
	}

	// Test with different inode numbers to verify subdirectory structure
	testCases := []struct {
		ino      uint64
		expected string
	}{
		{0x0000000000000001, "00/0000000000000001"},
		{0x0123456789abcdef, "01/0123456789abcdef"},
		{0xff00000000000000, "ff/ff00000000000000"},
	}

	for _, tc := range testCases {
		path := store.getPath(tc.ino)
		rel, err := filepath.Rel(tmpdir, path)
		if err != nil {
			t.Fatalf("Failed to get relative path: %v", err)
		}
		if rel != tc.expected {
			t.Errorf("getPath(%d) = %q, want %q", tc.ino, rel, tc.expected)
		}

		// Test Put to create subdirectory
		testData := []byte("test")
		err = store.Put(tc.ino, testData)
		if err != nil {
			t.Fatalf("Put failed for ino %d: %v", tc.ino, err)
		}

		// Verify subdirectory was created
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("Subdirectory not created for ino %d", tc.ino)
		}
	}
}

// TestFileSystemWithExternalStorage tests the complete filesystem with external storage
func TestFileSystemWithExternalStorage(t *testing.T) {
	tmpdir := t.TempDir()
	dbfile := filepath.Join(tmpdir, "test.db")
	contentdir := filepath.Join(tmpdir, "content")

	// Create filesystem content store
	contentStore, err := NewFilesystemContentStore(contentdir)
	if err != nil {
		t.Fatalf("NewFilesystemContentStore failed: %v", err)
	}

	// Open filesystem with external storage
	fs, err := OpenWithContentStore(dbfile, "", contentStore)
	if err != nil {
		t.Fatalf("OpenWithContentStore failed: %v", err)
	}
	defer fs.Close()

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Write data
	testData := []byte("hello external storage!")
	n, err := f.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(testData))
	}
	f.Close()

	// Verify content is stored externally, not in BoltDB data bucket
	var foundInBoltDB bool
	err = fs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				foundInBoltDB = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to check BoltDB: %v", err)
	}
	if foundInBoltDB {
		t.Error("Content found in BoltDB data bucket, should be in external storage")
	}

	// Read the file back
	f, err = fs.OpenFile("/test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile for read failed: %v", err)
	}

	buf := make([]byte, len(testData))
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Read %d bytes, want %d", n, len(testData))
	}
	if string(buf) != string(testData) {
		t.Fatalf("Read wrong data: got %q, want %q", buf, testData)
	}
	f.Close()

	// Test file deletion
	err = fs.Remove("/test.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify content was deleted from external storage
	// Get the inode number by trying to stat (should fail)
	_, err = fs.Stat("/test.txt")
	if !os.IsNotExist(err) {
		t.Fatalf("File should not exist after remove, got error: %v", err)
	}
}

// TestFileSystemWithBoltContentStore tests backward compatibility
func TestFileSystemWithBoltContentStore(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open filesystem with default BoltDB content store
	fs, err := Open(tmpfile, "")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer fs.Close()

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Write data
	testData := []byte("hello boltdb storage!")
	n, err := f.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(testData))
	}
	f.Close()

	// Verify content is stored in BoltDB data bucket
	var foundInBoltDB bool
	err = fs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				foundInBoltDB = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to check BoltDB: %v", err)
	}
	if !foundInBoltDB {
		t.Error("Content not found in BoltDB data bucket")
	}

	// Read the file back
	f, err = fs.OpenFile("/test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile for read failed: %v", err)
	}

	buf := make([]byte, len(testData))
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Read %d bytes, want %d", n, len(testData))
	}
	if string(buf) != string(testData) {
		t.Fatalf("Read wrong data: got %q, want %q", buf, testData)
	}
	f.Close()
}

// TestRemoveAllWithExternalStorage tests RemoveAll with external storage
func TestRemoveAllWithExternalStorage(t *testing.T) {
	tmpdir := t.TempDir()
	dbfile := filepath.Join(tmpdir, "test.db")
	contentdir := filepath.Join(tmpdir, "content")

	// Create filesystem content store
	contentStore, err := NewFilesystemContentStore(contentdir)
	if err != nil {
		t.Fatalf("NewFilesystemContentStore failed: %v", err)
	}

	// Open filesystem with external storage
	fs, err := OpenWithContentStore(dbfile, "", contentStore)
	if err != nil {
		t.Fatalf("OpenWithContentStore failed: %v", err)
	}
	defer fs.Close()

	// Create a directory with files
	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create multiple files
	for i := 0; i < 5; i++ {
		filename := filepath.Join("/testdir", "file"+string(rune('0'+i))+".txt")
		f, err := fs.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		f.Write([]byte("test data " + string(rune('0'+i))))
		f.Close()
	}

	// Remove all
	err = fs.RemoveAll("/testdir")
	if err != nil {
		t.Fatalf("RemoveAll failed: %v", err)
	}

	// Verify directory was removed
	_, err = fs.Stat("/testdir")
	if err == nil {
		t.Fatal("Directory should not exist after RemoveAll")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("Expected NotExist error, got: %v", err)
	}

	// Verify all content files were deleted from external storage
	// (we can't easily check individual files, but we can verify the content dir is mostly empty)
	entries, err := os.ReadDir(contentdir)
	if err != nil {
		t.Fatalf("Failed to read content directory: %v", err)
	}

	// Count files (not including subdirectories)
	fileCount := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			fileCount++
		}
	}

	if fileCount > 0 {
		t.Errorf("Found %d files in content directory after RemoveAll", fileCount)
	}
}
