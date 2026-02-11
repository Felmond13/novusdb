//go:build windows

package storage

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock = 0x00000002
	lockfileFailImmediate = 0x00000001
)

// fileLock represents an OS-level file lock (Windows implementation).
type fileLock struct {
	file *os.File
}

// lockFile acquires an exclusive lock on the given file path.
// Returns a fileLock that must be released with unlock().
func lockFile(path string) (*fileLock, error) {
	lockPath := path + ".lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("filelock: cannot open lock file: %w", err)
	}

	ol := new(syscall.Overlapped)
	r1, _, err := procLockFileEx.Call(
		f.Fd(),
		uintptr(lockfileExclusiveLock|lockfileFailImmediate),
		0,
		1, 0,
		uintptr(unsafe.Pointer(ol)),
	)
	if r1 == 0 {
		f.Close()
		return nil, fmt.Errorf("filelock: database %q is locked by another process", path)
	}

	return &fileLock{file: f}, nil
}

// unlock releases the file lock.
func (fl *fileLock) unlock() error {
	if fl.file == nil {
		return nil
	}
	ol := new(syscall.Overlapped)
	procUnlockFileEx.Call(
		fl.file.Fd(),
		0,
		1, 0,
		uintptr(unsafe.Pointer(ol)),
	)
	err := fl.file.Close()
	if fl.file != nil {
		os.Remove(fl.file.Name())
	}
	return err
}
