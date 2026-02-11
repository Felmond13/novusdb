package storage

import "testing"

func TestLRUCacheBasic(t *testing.T) {
	c := newLRUCache(3)

	var d1, d2, d3, d4 [PageSize]byte
	d1[0] = 1
	d2[0] = 2
	d3[0] = 3
	d4[0] = 4

	c.put(1, d1)
	c.put(2, d2)
	c.put(3, d3)

	// Toutes les 3 pages doivent être en cache
	if _, ok := c.get(1); !ok {
		t.Error("page 1 should be cached")
	}
	if _, ok := c.get(2); !ok {
		t.Error("page 2 should be cached")
	}
	if _, ok := c.get(3); !ok {
		t.Error("page 3 should be cached")
	}

	// Ajouter page 4 → page 1 devrait être évincée (LRU après les gets ci-dessus: 2 est LRU)
	// En fait après get(1), get(2), get(3) → l'ordre MRU est 3,2,1. Donc 1 est LRU.
	// Mais get(1) l'a remonté. Ordre MRU: 3,2,1 → get(1) → 1,3,2 → get(2) → 2,1,3 → get(3) → 3,2,1
	// Donc LRU = 1. Ajout de 4 → évince 1.
	c.put(4, d4)

	if _, ok := c.get(1); ok {
		t.Error("page 1 should have been evicted")
	}
	if _, ok := c.get(4); !ok {
		t.Error("page 4 should be cached")
	}
}

func TestLRUCacheUpdate(t *testing.T) {
	c := newLRUCache(3)

	var d1, d1new [PageSize]byte
	d1[0] = 1
	d1new[0] = 99

	c.put(1, d1)
	c.put(1, d1new) // mise à jour

	data, ok := c.get(1)
	if !ok {
		t.Fatal("page 1 should be cached")
	}
	if data[0] != 99 {
		t.Errorf("expected updated value 99, got %d", data[0])
	}
}

func TestLRUCacheInvalidate(t *testing.T) {
	c := newLRUCache(3)

	var d1 [PageSize]byte
	d1[0] = 1
	c.put(1, d1)

	c.invalidate(1)

	if _, ok := c.get(1); ok {
		t.Error("page 1 should have been invalidated")
	}
}

func TestLRUCacheClear(t *testing.T) {
	c := newLRUCache(3)

	var d [PageSize]byte
	c.put(1, d)
	c.put(2, d)
	c.put(3, d)

	c.clear()

	hits, misses, size, cap := c.stats()
	if size != 0 {
		t.Errorf("expected size 0 after clear, got %d", size)
	}
	_ = hits
	_ = misses
	_ = cap
}

func TestLRUCacheStats(t *testing.T) {
	c := newLRUCache(10)

	var d [PageSize]byte
	c.put(1, d)
	c.put(2, d)

	c.get(1) // hit
	c.get(1) // hit
	c.get(3) // miss

	hits, misses, size, cap := c.stats()
	if hits != 2 {
		t.Errorf("expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("expected 1 miss, got %d", misses)
	}
	if size != 2 {
		t.Errorf("expected size 2, got %d", size)
	}
	if cap != 10 {
		t.Errorf("expected capacity 10, got %d", cap)
	}

	rate := c.hitRate()
	if rate < 0.66 || rate > 0.67 {
		t.Errorf("expected hit rate ~0.667, got %f", rate)
	}
}

func TestLRUCacheEvictionOrder(t *testing.T) {
	c := newLRUCache(3)

	var d [PageSize]byte
	c.put(1, d)
	c.put(2, d)
	c.put(3, d)

	// Accéder à 1 pour le rendre MRU → ordre LRU: 2,3,1
	c.get(1)

	// Ajouter 4 → devrait évincer 2 (LRU)
	c.put(4, d)

	if _, ok := c.get(2); ok {
		t.Error("page 2 should have been evicted (LRU)")
	}
	if _, ok := c.get(1); !ok {
		t.Error("page 1 should still be cached (was accessed recently)")
	}
	if _, ok := c.get(3); !ok {
		t.Error("page 3 should still be cached")
	}
	if _, ok := c.get(4); !ok {
		t.Error("page 4 should be cached")
	}
}
