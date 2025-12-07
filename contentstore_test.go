package boltfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/absfs/memfs"
	bolt "go.etcd.io/bbolt"
)

// TestFileSystemWithMemFS tests using memfs for external content storage
func TestFileSystemWithMemFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Create memfs for content storage
	contentFS, err := memfs.NewFS()
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Open filesystem with memfs content storage
	fs, err := OpenWithContentFS(tmpfile, "", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS failed: %v", err)
	}
	defer fs.Close()

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	// Write data
	testData := []byte("hello memfs storage!")
	n, err := f.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("Write wrote %d bytes, want %d", n, len(testData))
	}
	f.Close()

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

	// Verify content is stored in memfs, not BoltDB
	// Get the inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Check that content exists in memfs
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in memfs: %v", err)
	}

	// Test file deletion
	err = fs.Remove("/test.txt")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Verify file was deleted
	_, err = fs.Stat("/test.txt")
	if !os.IsNotExist(err) {
		t.Fatalf("File should not exist after remove, got error: %v", err)
	}

	// Verify content was deleted from memfs
	_, err = contentFS.Stat(contentPath)
	if !os.IsNotExist(err) {
		t.Fatalf("Content should not exist in memfs after remove, got error: %v", err)
	}
}

// TestFileSystemWithoutExternalStorage tests backward compatibility
func TestFileSystemWithoutExternalStorage(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open filesystem without external storage (uses BoltDB by default)
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

// TestRemoveAllWithExternalStorage tests RemoveAll with memfs
func TestRemoveAllWithExternalStorage(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Create memfs for content storage
	contentFS, err := memfs.NewFS()
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Open filesystem with memfs content storage
	fs, err := OpenWithContentFS(tmpfile, "", contentFS)
	if err != nil {
		t.Fatalf("OpenWithContentFS failed: %v", err)
	}
	defer fs.Close()

	// Create a directory with files
	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Create multiple files
	var inodes []uint64
	for i := 0; i < 5; i++ {
		filename := filepath.Join("/testdir", "file"+string(rune('0'+i))+".txt")
		f, err := fs.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		f.Write([]byte("test data " + string(rune('0'+i))))

		// Get inode number
		info, _ := fs.Stat(filename)
		if node, ok := info.Sys().(*iNode); ok {
			inodes = append(inodes, node.Ino)
		}

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

	// Verify all content files were deleted from memfs
	for _, ino := range inodes {
		contentPath := inoToPath(ino)
		info, err := contentFS.Stat(contentPath)
		if !os.IsNotExist(err) {
			if info != nil {
				t.Errorf("Content for inode %d (path %s) should not exist in memfs, but found file with size %d, got error: %v", ino, contentPath, info.Size(), err)
			} else {
				t.Errorf("Content for inode %d (path %s) should not exist in memfs, got error: %v", ino, contentPath, err)
			}
		}
	}
}

// TestSetContentFS tests setting content filesystem on existing filesystem
func TestSetContentFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open filesystem without external storage
	fs, err := Open(tmpfile, "")
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer fs.Close()

	// Create memfs for content storage
	contentFS, err := memfs.NewFS()
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Set content filesystem
	fs.SetContentFS(contentFS)

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	testData := []byte("test data")
	f.Write(testData)
	f.Close()

	// Get inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Verify content exists in memfs
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in memfs: %v", err)
	}
}

// TestNewFSWithContentFS tests creating filesystem with content filesystem
func TestNewFSWithContentFS(t *testing.T) {
	tmpfile := filepath.Join(t.TempDir(), "test.db")

	// Open BoltDB
	db, err := bolt.Open(tmpfile, 0644, nil)
	if err != nil {
		t.Fatalf("bolt.Open failed: %v", err)
	}
	defer db.Close()

	// Create memfs for content storage
	contentFS, err := memfs.NewFS()
	if err != nil {
		t.Fatalf("NewFS failed: %v", err)
	}

	// Create filesystem with content filesystem
	fs, err := NewFSWithContentFS(db, "", contentFS)
	if err != nil {
		t.Fatalf("NewFSWithContentFS failed: %v", err)
	}

	// Create a test file
	f, err := fs.OpenFile("/test.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}

	testData := []byte("test data")
	f.Write(testData)
	f.Close()

	// Get inode number
	info, err := fs.Stat("/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	node, ok := info.Sys().(*iNode)
	if !ok {
		t.Fatal("Failed to get inode")
	}

	// Verify content exists in memfs
	contentPath := inoToPath(node.Ino)
	_, err = contentFS.Stat(contentPath)
	if err != nil {
		t.Fatalf("Content not found in memfs: %v", err)
	}
}
