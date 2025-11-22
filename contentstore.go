package boltfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

// ContentStore is an interface for storing and retrieving file content.
// This abstraction allows file content to be stored in different backends
// (BoltDB, filesystem, S3, etc.) while keeping metadata in BoltDB.
type ContentStore interface {
	// Put stores file content for the given inode number.
	Put(ino uint64, data []byte) error

	// Get retrieves file content for the given inode number.
	// Returns nil if content doesn't exist.
	Get(ino uint64) ([]byte, error)

	// Delete removes file content for the given inode number.
	Delete(ino uint64) error

	// Exists checks if content exists for the given inode number.
	Exists(ino uint64) bool

	// Close closes the content store and releases any resources.
	Close() error
}

// BoltContentStore stores file content directly in BoltDB.
// This is the default implementation and maintains backward compatibility.
type BoltContentStore struct {
	db *bolt.DB
}

// NewBoltContentStore creates a new BoltDB-backed content store.
func NewBoltContentStore(db *bolt.DB) *BoltContentStore {
	return &BoltContentStore{db: db}
}

// Put stores file content in the BoltDB data bucket.
func (s *BoltContentStore) Put(ino uint64, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return fmt.Errorf("data bucket not found")
		}
		return b.Put(i2b(ino), data)
	})
}

// Get retrieves file content from the BoltDB data bucket.
func (s *BoltContentStore) Get(ino uint64) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return fmt.Errorf("data bucket not found")
		}
		v := b.Get(i2b(ino))
		if v != nil {
			data = make([]byte, len(v))
			copy(data, v)
		}
		return nil
	})
	return data, err
}

// Delete removes file content from the BoltDB data bucket.
func (s *BoltContentStore) Delete(ino uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return fmt.Errorf("data bucket not found")
		}
		return b.Delete(i2b(ino))
	})
}

// Exists checks if content exists in the BoltDB data bucket.
func (s *BoltContentStore) Exists(ino uint64) bool {
	exists := false
	s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("data"))
		if b == nil {
			return nil
		}
		exists = b.Get(i2b(ino)) != nil
		return nil
	})
	return exists
}

// Close is a no-op for BoltContentStore since the DB is managed externally.
func (s *BoltContentStore) Close() error {
	return nil
}

// FilesystemContentStore stores file content in a filesystem directory.
// Each file's content is stored in a file named by its inode number.
type FilesystemContentStore struct {
	basePath string
}

// NewFilesystemContentStore creates a new filesystem-backed content store.
// The basePath directory will be created if it doesn't exist.
func NewFilesystemContentStore(basePath string) (*FilesystemContentStore, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create content store directory: %w", err)
	}
	return &FilesystemContentStore{basePath: basePath}, nil
}

// getPath returns the filesystem path for a given inode number.
func (s *FilesystemContentStore) getPath(ino uint64) string {
	// Use subdirectories to avoid too many files in a single directory
	// Format: basePath/XX/XXXXXXXXXXXXXXXX where XX is the first 2 hex digits
	hex := fmt.Sprintf("%016x", ino)
	return filepath.Join(s.basePath, hex[:2], hex)
}

// Put stores file content in the filesystem.
func (s *FilesystemContentStore) Put(ino uint64, data []byte) error {
	path := s.getPath(ino)

	// Create parent directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Write file atomically using a temporary file
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to write data: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to sync data: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Atomically rename temp file to final path
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// Get retrieves file content from the filesystem.
func (s *FilesystemContentStore) Get(ino uint64) ([]byte, error) {
	path := s.getPath(ino)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return data, nil
}

// Delete removes file content from the filesystem.
func (s *FilesystemContentStore) Delete(ino uint64) error {
	path := s.getPath(ino)

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	// Try to remove empty parent directory (ignore errors)
	os.Remove(filepath.Dir(path))

	return nil
}

// Exists checks if content exists in the filesystem.
func (s *FilesystemContentStore) Exists(ino uint64) bool {
	path := s.getPath(ino)
	_, err := os.Stat(path)
	return err == nil
}

// Close is a no-op for FilesystemContentStore.
func (s *FilesystemContentStore) Close() error {
	return nil
}
