package concurrency

import (
	"sync"
	"testing"
	"time"
)

func TestAcquireRelease(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)

	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	lm.ReleaseRecord("col", 1)

	// Doit pouvoir ré-acquérir après release
	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	lm.ReleaseRecord("col", 1)
}

func TestLockPolicyFail(t *testing.T) {
	lm := NewLockManager(LockPolicyFail)

	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// Deuxième acquire doit échouer immédiatement
	err := lm.AcquireRecord("col", 1)
	if err == nil {
		t.Fatal("expected error on second acquire with LockPolicyFail")
	}

	lm.ReleaseRecord("col", 1)

	// Après release, doit pouvoir acquérir
	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	lm.ReleaseRecord("col", 1)
}

func TestLockPolicyWait(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)
	lm.SetTimeout(2 * time.Second)

	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Goroutine qui attend puis libère
	go func() {
		time.Sleep(100 * time.Millisecond)
		lm.ReleaseRecord("col", 1)
	}()

	// Doit attendre et obtenir le lock après release
	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("waited acquire: %v", err)
	}
	lm.ReleaseRecord("col", 1)
}

func TestLockTimeout(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)
	lm.SetTimeout(100 * time.Millisecond)

	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// Ne pas libérer → le second acquire doit timeout
	err := lm.AcquireRecord("col", 1)
	if err == nil {
		t.Fatal("expected timeout error")
	}

	lm.ReleaseRecord("col", 1)
}

func TestDifferentRecordsNoContention(t *testing.T) {
	lm := NewLockManager(LockPolicyFail)

	// Acquérir des locks sur des records différents ne doit pas bloquer
	if err := lm.AcquireRecord("col", 1); err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if err := lm.AcquireRecord("col", 2); err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if err := lm.AcquireRecord("other", 1); err != nil {
		t.Fatalf("acquire other/1: %v", err)
	}

	lm.ReleaseRecord("col", 1)
	lm.ReleaseRecord("col", 2)
	lm.ReleaseRecord("other", 1)
}

func TestConcurrentLockDifferentRecords(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)
	lm.SetTimeout(5 * time.Second)

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// 20 goroutines, chacune lock/unlock un record différent
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := lm.AcquireRecord("col", id); err != nil {
					errCh <- err
					return
				}
				lm.ReleaseRecord("col", id)
			}
		}(uint64(i))
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("lock error: %v", err)
	}
}

func TestConcurrentLockSameRecord(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)
	lm.SetTimeout(5 * time.Second)

	var wg sync.WaitGroup
	counter := 0

	// 10 goroutines incrémentent un compteur protégé par le lock
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := lm.AcquireRecord("col", 1); err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				counter++
				lm.ReleaseRecord("col", 1)
			}
		}()
	}

	wg.Wait()

	if counter != 1000 {
		t.Errorf("expected counter=1000, got %d", counter)
	}
}

func TestReleaseWithoutAcquire(t *testing.T) {
	lm := NewLockManager(LockPolicyWait)
	// Ne doit pas paniquer
	lm.ReleaseRecord("col", 999)
}
