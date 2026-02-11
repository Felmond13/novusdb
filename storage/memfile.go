package storage

import (
	"io"
	"os"
	"sync"
	"time"
)

// StorageFile abstracts file operations for both native (os.File) and in-memory targets.
type StorageFile interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
}

// MemFile implements StorageFile backed by a byte slice (in-memory).
type MemFile struct {
	mu   sync.RWMutex
	data []byte
}

// NewMemFile creates a new empty in-memory file.
func NewMemFile() *MemFile {
	return &MemFile{}
}

func (m *MemFile) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MemFile) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	end := off + int64(len(p))
	if end > int64(len(m.data)) {
		grown := make([]byte, end)
		copy(grown, m.data)
		m.data = grown
	}
	n := copy(m.data[off:], p)
	return n, nil
}

func (m *MemFile) Sync() error  { return nil }
func (m *MemFile) Close() error { return nil }

func (m *MemFile) Stat() (os.FileInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &memFileInfo{size: int64(len(m.data))}, nil
}

// memFileInfo implements os.FileInfo for MemFile.
type memFileInfo struct{ size int64 }

func (fi *memFileInfo) Name() string      { return "memfile" }
func (fi *memFileInfo) Size() int64       { return fi.size }
func (fi *memFileInfo) Mode() os.FileMode { return 0644 }
func (fi *memFileInfo) ModTime() time.Time { return time.Time{} }
func (fi *memFileInfo) IsDir() bool       { return false }
func (fi *memFileInfo) Sys() interface{}  { return nil }
