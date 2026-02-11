//go:build !windows && !js && !wasip1

package storage

import (
	"fmt"
	"os"
	"syscall"
)

// fileLock represents an OS-level file lock (Unix implementation using flock).
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

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
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
	syscall.Flock(int(fl.file.Fd()), syscall.LOCK_UN)
	name := fl.file.Name()
	err := fl.file.Close()
	os.Remove(name)
	return err
}
