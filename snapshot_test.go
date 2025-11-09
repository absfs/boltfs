package boltfs

import (
	"os"
	"testing"
)

// tempFile helper is defined in cache_test.go

func TestSnapshot_Create(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	if snap.Name() != "test" {
		t.Errorf("expected name 'test', got '%s'", snap.Name())
	}

	if snap.Created().IsZero() {
		t.Error("created time should be set")
	}
}

func TestSnapshot_Release(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}

	err = snap.Release()
	if err != nil {
		t.Error(err)
	}

	// Using snapshot after release should fail
	_, err = snap.Stat("/")
	if err != os.ErrClosed {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	// Multiple releases should be safe
	err = snap.Release()
	if err != nil {
		t.Error("multiple releases should be safe")
	}
}

func TestSnapshot_Stat(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("hello"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Stat the file in snapshot
	info, err := snap.Stat("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info.Name() != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", info.Name())
	}
	if info.Size() != 5 {
		t.Errorf("expected size 5, got %d", info.Size())
	}
	if info.IsDir() {
		t.Error("file should not be a directory")
	}
}

func TestSnapshot_IsolatedFromWrites(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("original"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Modify the file in main filesystem
	file, err = fs.OpenFile("/test.txt", os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("modified"))
	file.Close()

	// Read from main filesystem
	mainData, err := readFileContents(fs, "/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(mainData) != "modified" {
		t.Errorf("main fs should have 'modified', got '%s'", string(mainData))
	}

	// Read from snapshot - should have original data
	snapData, err := snap.ReadFile("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(snapData) != "original" {
		t.Errorf("snapshot should have 'original', got '%s'", string(snapData))
	}
}

func TestSnapshot_ReadDir(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create directory with files
	err = fs.Mkdir("/testdir", 0755)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		file, err := fs.Create("/testdir/file" + string(rune('0'+i)) + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		file.Close()
	}

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read directory from snapshot
	infos, err := snap.ReadDir("/testdir")
	if err != nil {
		t.Fatal(err)
	}

	if len(infos) != 3 {
		t.Errorf("expected 3 entries, got %d", len(infos))
	}
}

func TestSnapshot_ReadFile(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create file with content
	file, err := fs.Create("/test.txt")
	if err != nil {
		t.Fatal(err)
	}
	testData := []byte("Hello, World!")
	file.Write(testData)
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read file from snapshot
	data, err := snap.ReadFile("/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != string(testData) {
		t.Errorf("expected '%s', got '%s'", string(testData), string(data))
	}
}

func TestSnapshot_Readlink(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create a file and a symlink
	file, err := fs.Create("/target.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	err = fs.Symlink("/target.txt", "/link.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Read symlink from snapshot
	target, err := snap.Readlink("/link.txt")
	if err != nil {
		t.Fatal(err)
	}

	if target != "/target.txt" {
		t.Errorf("expected '/target.txt', got '%s'", target)
	}
}

func TestSnapshot_Walk(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create directory structure
	err = fs.MkdirAll("/a/b/c", 0755)
	if err != nil {
		t.Fatal(err)
	}

	file, err := fs.Create("/a/file1.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	file, err = fs.Create("/a/b/file2.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Walk the snapshot
	visited := make(map[string]bool)
	err = snap.Walk("/a", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		visited[path] = true
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"/a", "/a/b", "/a/b/c", "/a/b/file2.txt", "/a/file1.txt"}
	for _, path := range expected {
		if !visited[path] {
			t.Errorf("expected to visit '%s'", path)
		}
	}
}

func TestSnapshot_WriteOperationsFail(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// All write operations should fail
	if err := snap.Mkdir("/test", 0755); err != os.ErrPermission {
		t.Errorf("Mkdir should return ErrPermission, got %v", err)
	}

	if _, err := snap.Create("/test"); err != os.ErrPermission {
		t.Errorf("Create should return ErrPermission, got %v", err)
	}

	if err := snap.Remove("/test"); err != os.ErrPermission {
		t.Errorf("Remove should return ErrPermission, got %v", err)
	}
}

func TestSnapshotManager_CreateAndGet(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	snap, err := sm.Create("test1")
	if err != nil {
		t.Fatal(err)
	}

	if snap.Name() != "test1" {
		t.Errorf("expected name 'test1', got '%s'", snap.Name())
	}

	// Get the snapshot
	retrieved, ok := sm.Get("test1")
	if !ok {
		t.Fatal("snapshot should exist")
	}

	if retrieved.Name() != snap.Name() {
		t.Error("retrieved snapshot should match created snapshot")
	}

	// Try to get non-existent snapshot
	_, ok = sm.Get("nonexistent")
	if ok {
		t.Error("non-existent snapshot should not be found")
	}
}

func TestSnapshotManager_CreateDuplicate(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	_, err = sm.Create("test")
	if err != nil {
		t.Fatal(err)
	}

	// Try to create duplicate
	_, err = sm.Create("test")
	if err != os.ErrExist {
		t.Errorf("expected ErrExist, got %v", err)
	}
}

func TestSnapshotManager_Delete(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	snap, err := sm.Create("test")
	if err != nil {
		t.Fatal(err)
	}

	err = sm.Delete("test")
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot should no longer exist
	_, ok := sm.Get("test")
	if ok {
		t.Error("snapshot should have been deleted")
	}

	// Snapshot should be released
	_, err = snap.Stat("/")
	if err != os.ErrClosed {
		t.Error("snapshot should be released after delete")
	}

	// Delete non-existent should fail
	err = sm.Delete("nonexistent")
	if err != os.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestSnapshotManager_List(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	// Empty list
	if len(sm.List()) != 0 {
		t.Error("list should be empty initially")
	}

	// Create snapshots
	names := []string{"snap1", "snap2", "snap3"}
	for _, name := range names {
		_, err := sm.Create(name)
		if err != nil {
			t.Fatal(err)
		}
	}

	list := sm.List()
	if len(list) != 3 {
		t.Errorf("expected 3 snapshots, got %d", len(list))
	}

	// Check all names are present
	listMap := make(map[string]bool)
	for _, name := range list {
		listMap[name] = true
	}

	for _, name := range names {
		if !listMap[name] {
			t.Errorf("expected snapshot '%s' in list", name)
		}
	}
}

func TestSnapshotManager_ReleaseAll(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	sm := fs.NewSnapshotManager()

	// Create multiple snapshots
	snaps := make([]*Snapshot, 3)
	for i := 0; i < 3; i++ {
		snap, err := sm.Create("snap" + string(rune('1'+i)))
		if err != nil {
			t.Fatal(err)
		}
		snaps[i] = snap
	}

	err = sm.ReleaseAll()
	if err != nil {
		t.Fatal(err)
	}

	// All snapshots should be released
	for _, snap := range snaps {
		_, err := snap.Stat("/")
		if err != os.ErrClosed {
			t.Error("snapshot should be released")
		}
	}

	// List should be empty
	if len(sm.List()) != 0 {
		t.Error("list should be empty after ReleaseAll")
	}
}

func TestSnapshot_CopyToFS(t *testing.T) {
	path := tempFile()
	defer os.Remove(path)

	fs, err := Open(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Create original file
	file, err := fs.Create("/original.txt")
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("original content"))
	file.Close()

	// Create snapshot
	snap, err := fs.CreateSnapshot("test")
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Release()

	// Modify original file
	file, err = fs.OpenFile("/original.txt", os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte("modified"))
	file.Close()

	// Copy from snapshot to new location
	err = snap.CopyToFS("/original.txt", "/restored.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Check restored file has snapshot content
	data, err := readFileContents(fs, "/restored.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "original content" {
		t.Errorf("expected 'original content', got '%s'", string(data))
	}

	// Original file should still be modified
	data, err = readFileContents(fs, "/original.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "modified" {
		t.Errorf("expected 'modified', got '%s'", string(data))
	}
}

// Helper function to read file contents
func readFileContents(fs *FileSystem, path string) ([]byte, error) {
	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	data := make([]byte, info.Size())
	_, err = file.Read(data)
	return data, err
}
