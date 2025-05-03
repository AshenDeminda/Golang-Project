package kv

import (
	"container/list"
	"sync"

	bolt "go.etcd.io/bbolt"
)

/* ──────────────── public commands ──────────────── */
type SetCmd struct{ Key, Value string }
type DelCmd struct{ Key string }
type GetCmd struct{ Key string }

/* ──────────────── Store struct ─────────────────── */
type Store struct {
	db  *bolt.DB
	lru *lruCache
}

func New(db *bolt.DB, maxEntries int) *Store {
	_ = db.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket([]byte("kv"))
		_, _ = tx.CreateBucket([]byte("kv"))
		return nil
	})
	return &Store{db: db, lru: newLRU(maxEntries)}
}

func (s *Store) Close() { _ = s.db.Close() }

/* ─────────────────── API ───────────────────────── */

func (s *Store) Get(key string) string {
	// First check cache
	if v, ok := s.lru.get(key); ok {
		return v
	}

	// If not in cache, check the database
	var value string
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte("kv")).Get([]byte(key)); v != nil {
			value = string(v)
			// Update the cache with the retrieved value
			s.lru.add(key, value)
		}
		return nil
	})
	return value
}

func (s *Store) Apply(cmd any) any {
	switch c := cmd.(type) {

	case SetCmd:
		_ = s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("kv")).Put([]byte(c.Key), []byte(c.Value))
		})
		// Update cache on Set
		s.lru.add(c.Key, c.Value)

	case DelCmd:
		_ = s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("kv")).Delete([]byte(c.Key))
		})
		// Remove from cache on Delete
		s.lru.remove(c.Key)

	case GetCmd:
		return s.Get(c.Key)
	}
	return nil
}

/* ───────────── LRU cache ──────────────── */
type entry struct{ key, value string }

type lruCache struct {
	mu  sync.Mutex
	ll  *list.List
	tab map[string]*list.Element
	cap int
}

func newLRU(cap int) *lruCache {
	return &lruCache{ll: list.New(), tab: make(map[string]*list.Element), cap: cap}
}

func (c *lruCache) get(k string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.tab[k]
	if !ok {
		return "", false
	}
	// Move accessed element to front (MRU position)
	c.ll.MoveToFront(e)
	return e.Value.(*entry).value, true
}

func (c *lruCache) add(k, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.tab[k]; ok {
		e.Value.(*entry).value = v
		c.ll.MoveToFront(e)
		return
	}
	// Add new entry to front of list
	e := c.ll.PushFront(&entry{k, v})
	c.tab[k] = e
	
	// Evict if over capacity
	if c.ll.Len() > c.cap {
		back := c.ll.Back()
		if back != nil {
			c.ll.Remove(back)
			delete(c.tab, back.Value.(*entry).key)
		}
	}
}

func (c *lruCache) remove(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.tab[k]; ok {
		c.ll.Remove(e)
		delete(c.tab, k)
	}
}