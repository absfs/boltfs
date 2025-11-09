package boltfs

import (
	"fmt"
	"os"
	"testing"
)

// Benchmark helpers
func setupBenchFS(b *testing.B, cacheSize int) (*FileSystem, func()) {
	path := fmt.Sprintf("bench_boltfs_%d.db", b.N)
	fs, err := Open(path, "")
	if err != nil {
		b.Fatal(err)
	}

	// Configure cache
	fs.SetCacheSize(cacheSize)

	cleanup := func() {
		fs.Close()
		os.Remove(path)
	}

	return fs, cleanup
}

func createBenchFiles(b *testing.B, fs *FileSystem, numFiles int) {
	b.Helper()
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("/file_%d.txt", i)
		file, err := fs.OpenFile(name, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			b.Fatalf("creating file %s: %v", name, err)
		}
		data := []byte(fmt.Sprintf("content for file %d", i))
		_, err = file.Write(data)
		if err != nil {
			file.Close()
			b.Fatalf("writing to file %s: %v", name, err)
		}
		file.Close()
	}
}

func createBenchDirs(b *testing.B, fs *FileSystem, depth int) {
	b.Helper()
	path := "/"
	for i := 0; i < depth; i++ {
		path = fmt.Sprintf("%s/dir_%d", path, i)
		if err := fs.Mkdir(path, 0755); err != nil {
			b.Fatalf("creating dir %s: %v", path, err)
		}
	}
}

// Benchmark: File stat operations (cache benefit test)
func BenchmarkStat_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0) // No cache
	defer cleanup()

	createBenchFiles(b, fs, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(fmt.Sprintf("/file_%d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat_SmallCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 50) // Small cache
	defer cleanup()

	createBenchFiles(b, fs, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(fmt.Sprintf("/file_%d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat_LargeCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000) // Large cache
	defer cleanup()

	createBenchFiles(b, fs, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(fmt.Sprintf("/file_%d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Sequential stat operations (best case for cache)
func BenchmarkStat_Sequential_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	createBenchFiles(b, fs, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Repeatedly stat the same file (best case for cache)
		_, err := fs.Stat("/file_0.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat_Sequential_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchFiles(b, fs, 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Repeatedly stat the same file (best case for cache)
		_, err := fs.Stat("/file_0.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Path resolution (deep directory traversal)
func BenchmarkResolve_DeepPath_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	createBenchDirs(b, fs, 10)

	path := "/dir_0/dir_1/dir_2/dir_3/dir_4/dir_5/dir_6/dir_7/dir_8/dir_9"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkResolve_DeepPath_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchDirs(b, fs, 10)

	path := "/dir_0/dir_1/dir_2/dir_3/dir_4/dir_5/dir_6/dir_7/dir_8/dir_9"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := fs.Stat(path)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: File read operations
func BenchmarkRead_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	// Create a file with 1KB of data
	file, _ := fs.Create("/testfile.txt")
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	file.Write(data)
	file.Close()

	buf := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, err := fs.Open("/testfile.txt")
		if err != nil {
			b.Fatal(err)
		}
		file.Read(buf)
		file.Close()
	}
}

func BenchmarkRead_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	// Create a file with 1KB of data
	file, _ := fs.Create("/testfile.txt")
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	file.Write(data)
	file.Close()

	buf := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, err := fs.Open("/testfile.txt")
		if err != nil {
			b.Fatal(err)
		}
		file.Read(buf)
		file.Close()
	}
}

// Benchmark: Directory listing
func BenchmarkReadDir_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	// Create directory with 50 files
	fs.Mkdir("/testdir", 0755)
	for i := 0; i < 50; i++ {
		file, _ := fs.Create(fmt.Sprintf("/testdir/file_%d.txt", i))
		file.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir, err := fs.OpenFile("/testdir", os.O_RDONLY, 0)
		if err != nil {
			b.Fatal(err)
		}
		dir.Readdir(-1)
		dir.Close()
	}
}

func BenchmarkReadDir_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	// Create directory with 50 files
	fs.Mkdir("/testdir", 0755)
	for i := 0; i < 50; i++ {
		file, _ := fs.Create(fmt.Sprintf("/testdir/file_%d.txt", i))
		file.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dir, err := fs.OpenFile("/testdir", os.O_RDONLY, 0)
		if err != nil {
			b.Fatal(err)
		}
		dir.Readdir(-1)
		dir.Close()
	}
}

// Benchmark: File write operations (cache should not help much)
func BenchmarkWrite_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, err := fs.Create(fmt.Sprintf("/file_%d.txt", i))
		if err != nil {
			b.Fatal(err)
		}
		file.Write(data)
		file.Close()
	}
}

func BenchmarkWrite_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	data := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, err := fs.Create(fmt.Sprintf("/file_%d.txt", i))
		if err != nil {
			b.Fatal(err)
		}
		file.Write(data)
		file.Close()
	}
}

// Benchmark: Mixed read/write workload
func BenchmarkMixed_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	createBenchFiles(b, fs, 100)

	buf := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 80% reads, 20% writes (typical workload)
		if i%5 == 0 {
			// Write
			file, _ := fs.Create(fmt.Sprintf("/new_%d.txt", i))
			file.Write(buf)
			file.Close()
		} else {
			// Read
			file, _ := fs.Open(fmt.Sprintf("/file_%d.txt", i%100))
			file.Read(buf)
			file.Close()
		}
	}
}

func BenchmarkMixed_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchFiles(b, fs, 100)

	buf := make([]byte, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 80% reads, 20% writes (typical workload)
		if i%5 == 0 {
			// Write
			file, _ := fs.Create(fmt.Sprintf("/new_%d.txt", i))
			file.Write(buf)
			file.Close()
		} else {
			// Read
			file, _ := fs.Open(fmt.Sprintf("/file_%d.txt", i%100))
			file.Read(buf)
			file.Close()
		}
	}
}

// Benchmark: Snapshot creation
func BenchmarkSnapshot_Create(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchFiles(b, fs, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap, err := fs.CreateSnapshot(fmt.Sprintf("bench_%d", i))
		if err != nil {
			b.Fatal(err)
		}
		snap.Release()
	}
}

// Benchmark: Snapshot read operations
func BenchmarkSnapshot_Read(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchFiles(b, fs, 100)

	snap, err := fs.CreateSnapshot("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer snap.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := snap.ReadFile(fmt.Sprintf("/file_%d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Snapshot stat operations
func BenchmarkSnapshot_Stat(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	createBenchFiles(b, fs, 100)

	snap, err := fs.CreateSnapshot("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer snap.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := snap.Stat(fmt.Sprintf("/file_%d.txt", i%100))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark: Cache operations themselves
func BenchmarkCache_Put(b *testing.B) {
	cache := newInodeCache(1000)
	node := newInode(0644)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Put(uint64(i%1000), node, false)
	}
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	cache := newInodeCache(1000)
	node := newInode(0644)

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		cache.Put(uint64(i), node, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(uint64(i % 1000))
	}
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	cache := newInodeCache(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(uint64(i))
	}
}

// Benchmark: Walk operations
func BenchmarkWalk_NoCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 0)
	defer cleanup()

	// Create directory tree
	fs.MkdirAll("/a/b/c", 0755)
	for i := 0; i < 10; i++ {
		fs.Create(fmt.Sprintf("/a/file_%d.txt", i))
		fs.Create(fmt.Sprintf("/a/b/file_%d.txt", i))
		fs.Create(fmt.Sprintf("/a/b/c/file_%d.txt", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		fs.Walk("/a", func(path string, info os.FileInfo, err error) error {
			count++
			return nil
		})
	}
}

func BenchmarkWalk_WithCache(b *testing.B) {
	fs, cleanup := setupBenchFS(b, 1000)
	defer cleanup()

	// Create directory tree
	fs.MkdirAll("/a/b/c", 0755)
	for i := 0; i < 10; i++ {
		fs.Create(fmt.Sprintf("/a/file_%d.txt", i))
		fs.Create(fmt.Sprintf("/a/b/file_%d.txt", i))
		fs.Create(fmt.Sprintf("/a/b/c/file_%d.txt", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		fs.Walk("/a", func(path string, info os.FileInfo, err error) error {
			count++
			return nil
		})
	}
}

// Benchmark: Cache size comparison
func BenchmarkCacheSize_10(b *testing.B)   { benchmarkCacheSize(b, 10) }
func BenchmarkCacheSize_100(b *testing.B)  { benchmarkCacheSize(b, 100) }
func BenchmarkCacheSize_1000(b *testing.B) { benchmarkCacheSize(b, 1000) }
func BenchmarkCacheSize_5000(b *testing.B) { benchmarkCacheSize(b, 5000) }

func benchmarkCacheSize(b *testing.B, cacheSize int) {
	fs, cleanup := setupBenchFS(b, cacheSize)
	defer cleanup()

	// Create 1000 files
	createBenchFiles(b, fs, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access files in a pattern that tests cache efficiency
		for j := 0; j < 100; j++ {
			idx := (i*100 + j) % 1000
			_, err := fs.Stat(fmt.Sprintf("/file_%d.txt", idx))
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
