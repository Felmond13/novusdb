package storage

import "sync"

// lruCache implémente un cache LRU (Least Recently Used) pour les pages.
// Doubly-linked list + hash map pour O(1) get/put/evict.
// Thread-safe via son propre mutex (indépendant du mutex du Pager).
type lruCache struct {
	mu       sync.Mutex
	capacity int
	items    map[uint32]*lruNode
	head     *lruNode // MRU (most recently used)
	tail     *lruNode // LRU (least recently used)

	// Statistiques
	hits   uint64
	misses uint64
}

type lruNode struct {
	pageID uint32
	data   [PageSize]byte
	prev   *lruNode
	next   *lruNode
}

// newLRUCache crée un nouveau cache LRU avec la capacité donnée (nombre de pages).
func newLRUCache(capacity int) *lruCache {
	if capacity <= 0 {
		capacity = 256 // défaut : 256 pages = 1 MB (avec PageSize=4096)
	}
	return &lruCache{
		capacity: capacity,
		items:    make(map[uint32]*lruNode, capacity),
	}
}

// get retourne les données d'une page si elle est en cache.
func (c *lruCache) get(pageID uint32) ([PageSize]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.items[pageID]
	if !ok {
		c.misses++
		return [PageSize]byte{}, false
	}
	c.hits++
	c.moveToFront(node)
	return node.data, true
}

// put ajoute ou met à jour une page dans le cache.
// Si le cache est plein, évince la page LRU.
func (c *lruCache) put(pageID uint32, data [PageSize]byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if node, ok := c.items[pageID]; ok {
		// Mise à jour d'une entrée existante
		node.data = data
		c.moveToFront(node)
		return
	}

	// Nouvelle entrée
	node := &lruNode{pageID: pageID, data: data}
	c.items[pageID] = node
	c.pushFront(node)

	// Eviction si capacité dépassée
	if len(c.items) > c.capacity {
		c.evict()
	}
}

// invalidate supprime une page du cache.
func (c *lruCache) invalidate(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.items[pageID]
	if !ok {
		return
	}
	c.removeNode(node)
	delete(c.items, pageID)
}

// clear vide entièrement le cache.
func (c *lruCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[uint32]*lruNode, c.capacity)
	c.head = nil
	c.tail = nil
}

// stats retourne les statistiques du cache.
func (c *lruCache) stats() (hits, misses uint64, size, capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, len(c.items), c.capacity
}

// hitRate retourne le taux de hit du cache (0.0 à 1.0).
func (c *lruCache) hitRate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total)
}

// ---------- Opérations internes sur la liste chaînée ----------

func (c *lruCache) pushFront(node *lruNode) {
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
	if c.tail == nil {
		c.tail = node
	}
}

func (c *lruCache) removeNode(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (c *lruCache) moveToFront(node *lruNode) {
	if node == c.head {
		return // déjà en tête
	}
	c.removeNode(node)
	c.pushFront(node)
}

func (c *lruCache) evict() {
	if c.tail == nil {
		return
	}
	victim := c.tail
	c.removeNode(victim)
	delete(c.items, victim.pageID)
}
