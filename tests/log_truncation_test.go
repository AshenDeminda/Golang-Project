package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

// ---------------------------------------------------------------------
//
//	UNIT: boltLog.TruncateBefore
//
// --------------------------------------------------------------------
func TestBoltLogTruncateBefore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "log.bolt")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to open bolt DB: %v", err)
	}
	defer db.Close() // Ensure DB is closed
	
	logStore := raft.NewBoltLog(db)

	// append 100 entries
	for i := 1; i <= 100; i++ {
		logStore.Append(raft.LogEntry{Term: 1, Command: kv.SetCmd{
			Key: fmt.Sprintf("k%02d", i), Value: "v"}})
	}
	if logStore.FirstIndex() != 1 || logStore.LastIndex() != 100 {
		t.Fatalf("append failed")
	}

	// keep tail 40, prune before 61
	logStore.TruncateBefore(61)
	if got := logStore.FirstIndex(); got != 61 {
		t.Fatalf("firstIndex want 61 got %d", got)
	}
	if entry, ok := logStore.At(50); ok {
		t.Fatalf("expected entry 50 to be gone, but got: %v", entry)
	}
	if _, ok := logStore.At(80); !ok {
		t.Fatalf("expected entry 80 to exist")
	}

	// close + reopen → FirstIndex must persist
	db.Close()
	
	db2, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("Failed to reopen bolt DB: %v", err)
	}
	defer db2.Close() // Ensure DB is closed
	
	reopen := raft.NewBoltLog(db2)
	if reopen.FirstIndex() != 61 {
		t.Fatalf("persisted firstIndex lost; want 61 got %d", reopen.FirstIndex())
	}
}

// ---------------------------------------------------------------------
//
//	INTEGRATION: manual pruning
//
// ---------------------------------------------------------------------
func TestClusterManualPrune(t *testing.T) {
	nodes, stop := buildCluster(t, 1)
	defer stop()
	time.Sleep(3 * time.Second) // elect

	// find leader
	var leader *raft.Node
	for _, n := range nodes {
		if n != nil && n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader")
	}

	/* write 3 000 entries */
	for i := 1; i <= 3000; i++ {
		if _, ok := leader.Propose(kv.SetCmd{Key: fmt.Sprintf("x%04d", i), Value: "v"}); !ok {
			t.Fatalf("propose fail at %d", i)
		}
	}
	// wait leader apply
	deadline := time.Now().Add(10 * time.Second) // Add deadline to prevent infinite wait
	for leader.LastApplied() < 3000 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for leader to apply entries")
		}
		time.Sleep(5 * time.Millisecond)
	}

	/* --- prune the first 2500 entries on the leader --- */
	leader.Log().TruncateBefore(2501)

	// verify firstIndex reflects change
	if fi := leader.Log().FirstIndex(); fi != 2501 {
		t.Fatalf("prune failed: firstIndex=%d", fi)
	}

	/* followers should still replicate new command */
	if _, ok := leader.Propose(kv.SetCmd{Key: "tail", Value: "ok"}); !ok {
		t.Fatalf("post-prune propose failed")
	}
	deadline = time.Now().Add(5 * time.Second)
	for _, n := range nodes {
		if n == nil {
			continue
		}
		for n.LastApplied() < 3001 {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not catch up after prune", n.ID())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------
//
//	INTEGRATION: automatic pruning
//
// ---------------------------------------------------------------------
func TestClusterAutoPrune(t *testing.T) {
	// -------- build single-node cluster ----------
	nodes, stop := buildCluster(t, 1)
	defer stop()

	leader := nodes[0]
	time.Sleep(2 * time.Second) // self-elect

	// -------- flood just over pruneEvery ----------
	const total = 2100
	for i := 1; i <= total; i++ {
		if _, ok := leader.Propose(kv.SetCmd{
			Key: fmt.Sprintf("k%04d", i), Value: "x"}); !ok {
			t.Fatalf("propose %d failed", i)
		}
	}
	
	deadline := time.Now().Add(10 * time.Second) // Add deadline to prevent infinite wait
	for leader.LastApplied() < total {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for leader to apply entries")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// -------- auto-prune should have fired --------
	// Give time for auto-prune to complete
	time.Sleep(500 * time.Millisecond)
	
	fi := leader.Log().FirstIndex()
	wantMin := int(total/config.PruneEvery)*config.PruneEvery - config.RetainTail + 1
	if fi < wantMin {
		t.Fatalf("auto prune did not advance firstIndex; got %d, want ≥ %d",
			fi, wantMin)
	}

	// -------- cluster still usable after prune ----
	if _, ok := leader.Propose(kv.SetCmd{Key: "tail", Value: "ok"}); !ok {
		t.Fatalf("post-prune propose failed")
	}
	
	deadline = time.Now().Add(10 * time.Second)
	for leader.LastApplied() < total+1 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for leader to apply final entry")
		}
		time.Sleep(5 * time.Millisecond)
	}
}