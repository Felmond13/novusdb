// Package concurrency fournit un gestionnaire de verrous au niveau record.
package concurrency

import (
	"fmt"
	"sync"
	"time"
)

// LockPolicy définit le comportement quand un lock est déjà pris.
type LockPolicy int

const (
	LockPolicyWait LockPolicy = iota // attendre que le lock soit libéré
	LockPolicyFail                   // échouer immédiatement
)

// DefaultLockTimeout est le timeout par défaut pour l'acquisition d'un lock.
const DefaultLockTimeout = 5 * time.Second

// LockManager gère les verrous au niveau record et un verrou global pour l'index.
type LockManager struct {
	mu      sync.Mutex
	locks   map[lockKey]*recordLock
	policy  LockPolicy
	timeout time.Duration

	// IndexMu est un verrou coarse-grained pour les mises à jour d'index.
	IndexMu sync.Mutex
}

type lockKey struct {
	collection string
	recordID   uint64
}

type recordLock struct {
	mu      sync.Mutex
	holders int // pour les readers (non utilisé en v1, préparé pour v2)
	writer  bool
	cond    *sync.Cond
}

// NewLockManager crée un nouveau gestionnaire de verrous.
func NewLockManager(policy LockPolicy) *LockManager {
	return &LockManager{
		locks:   make(map[lockKey]*recordLock),
		policy:  policy,
		timeout: DefaultLockTimeout,
	}
}

// SetTimeout définit le timeout pour l'acquisition de locks.
func (lm *LockManager) SetTimeout(d time.Duration) {
	lm.timeout = d
}

// getOrCreateLock retourne le recordLock pour la clé donnée, en le créant si nécessaire.
func (lm *LockManager) getOrCreateLock(key lockKey) *recordLock {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	rl, ok := lm.locks[key]
	if !ok {
		rl = &recordLock{}
		rl.cond = sync.NewCond(&rl.mu)
		lm.locks[key] = rl
	}
	return rl
}

// AcquireRecord acquiert un verrou exclusif sur un record.
func (lm *LockManager) AcquireRecord(collection string, recordID uint64) error {
	key := lockKey{collection: collection, recordID: recordID}
	rl := lm.getOrCreateLock(key)

	if lm.policy == LockPolicyFail {
		rl.mu.Lock()
		if rl.writer {
			rl.mu.Unlock()
			return fmt.Errorf("lock: record %d in %q already locked", recordID, collection)
		}
		rl.writer = true
		rl.mu.Unlock()
		return nil
	}

	// LockPolicyWait : attendre avec timeout via cond.Wait dans une goroutine
	acquired := make(chan struct{})
	go func() {
		rl.mu.Lock()
		for rl.writer {
			rl.cond.Wait()
		}
		rl.writer = true
		rl.mu.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
		return nil
	case <-time.After(lm.timeout):
		return fmt.Errorf("lock: timeout acquiring lock on record %d in %q", recordID, collection)
	}
}

// ReleaseRecord libère le verrou exclusif sur un record.
func (lm *LockManager) ReleaseRecord(collection string, recordID uint64) {
	key := lockKey{collection: collection, recordID: recordID}

	lm.mu.Lock()
	rl, ok := lm.locks[key]
	lm.mu.Unlock()

	if !ok {
		return
	}

	rl.mu.Lock()
	rl.writer = false
	rl.cond.Broadcast()
	rl.mu.Unlock()
}
