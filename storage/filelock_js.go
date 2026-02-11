//go:build js || wasip1

package storage

// fileLock is a no-op on js/wasm (in-memory only, no file system).
type fileLock struct{}

// lockFile is a no-op on js/wasm.
func lockFile(_ string) (*fileLock, error) {
	return &fileLock{}, nil
}

// unlock is a no-op on js/wasm.
func (fl *fileLock) unlock() error {
	return nil
}
