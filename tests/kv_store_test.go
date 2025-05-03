package tests

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
)


// ---------------------------------------------------------
// 	helpers
// ---------------------------------------------------------
func newTempStore(t *testing.T, cacheSize int) (*kv.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kv.bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 0})
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	s := kv.New(db, cacheSize)
	return s, func() { 
		s.Close() 
		db.Close() // Ensure DB is also closed
	}
}


// ---------------------------------------------------------
//  1. basic Set / Get / Del
// ---------------------------------------------------------
func TestStoreBasicOps(t *testing.T) {
	s, clean := newTempStore(t, 10)
	defer clean()

	// set
	s.Apply(kv.SetCmd{Key: "foo", Value: "bar"})
	if got := s.Get("foo"); got != "bar" {
		t.Fatalf("want bar, got %q", got)
	}

	// delete
	s.Apply(kv.DelCmd{Key: "foo"})
	if got := s.Get("foo"); got != "" {
		t.Fatalf("delete failed, got %q", got)
	}
}

// ---------------------------------------------------------
//  2. LRU cache hit / eviction
// ---------------------------------------------------------
func TestStoreLRUEviction(t *testing.T) {
	s, clean := newTempStore(t, 2) // cache holds only 2
	defer clean()

	// Reset cache statistics before test
	s.ResetCacheStats() // Add this method to your Store
	
	// fill three keys
	s.Apply(kv.SetCmd{Key: "a", Value: "1"})
	s.Apply(kv.SetCmd{Key: "b", Value: "2"})
	s.Apply(kv.SetCmd{Key: "c", Value: "3"}) // evicts "a"

	// Verify "b" and "c" are in cache by checking hits
	beforeHits := s.CacheHits()
	_ = s.Get("b")
	_ = s.Get("c")
	afterHits := s.CacheHits()
	
	if afterHits - beforeHits != 2 {
		t.Fatalf("Expected 2 cache hits for keys 'b' and 'c', got %d", afterHits-beforeHits)
	}
	
	// "a" must trigger Bolt hit (cache miss) but still return correct value
	beforeMisses := s.CacheMisses()
	if got := s.Get("a"); got != "1" {
		t.Fatalf("want 1, got %q", got)
	}
	afterMisses := s.CacheMisses()
	
	if afterMisses - beforeMisses != 1 {
		t.Fatalf("Expected cache miss for key 'a' after eviction, but got %d misses", afterMisses-beforeMisses)
	}
}

// ---------------------------------------------------------
//  3. persistence across reopen
// ---------------------------------------------------------
func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "kv.bolt")

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open initial DB: %v", err)
	}
	
	store1 := kv.New(db, 5)
	store1.Apply(kv.SetCmd{Key: "x", Value: "y"})
	store1.Close()
	db.Close() // Explicitly close the db

	// reopen same file
	db2, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	
	store2 := kv.New(db2, 5)
	if got := store2.Get("x"); got != "y" {
		t.Fatalf("persistence lost: want y got %q", got)
	}
	store2.Close()
	db2.Close()
}

// ---------------------------------------------------------
//  4. concurrent writers & readers
// ---------------------------------------------------------
func TestStoreConcurrent(t *testing.T) {
	s, clean := newTempStore(t, 50)
	defer clean()

	var wg sync.WaitGroup
	var errorsMu sync.Mutex
	var errors []string
	
	for i := range 100 {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := "k" + string(rune(i))
			s.Apply(kv.SetCmd{Key: key, Value: "v"})
			if got := s.Get(key); got != "v" {
				errorsMu.Lock()
				errors = append(errors, fmt.Sprintf("concurrent get failed for %s", key))
				errorsMu.Unlock()
			}
			s.Apply(kv.DelCmd{Key: key})
		}()
	}
	wg.Wait()

	// Report collected errors
	for _, err := range errors {
		t.Errorf("%s", err)
	}

	// small delay to ensure deletes flushed
	time.Sleep(50 * time.Millisecond)
	if got := s.Get("k0"); got != "" {
		t.Fatalf("expected empty after delete, got %q", got)
	}
}