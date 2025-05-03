package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

// ---------------------------------------------------------
// leader crashes  →  follower takes over
// ---------------------------------------------------------
func TestLeaderFailover(t *testing.T) {
	nodes, stop := buildCluster(t, 3)
	defer stop()
	time.Sleep(1 * time.Second)

	ldrIdx, ldr := leaderOf(nodes)
	if ldr == nil {
		t.Fatalf("no leader elected")
	}

	// Properly clean up leader before killing it
	applyCh := ldr.ApplyCh()
	killNode(ldr) // crash leader
	
	// Drain the apply channel to prevent goroutine leaks
	go func() {
		for range applyCh {
			// Drain until closed
		}
	}()

	// restart old node (returns as follower)
	nodes[ldrIdx] = restartNode(t, nodes[ldrIdx])
	time.Sleep(1 * time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for {
		_, l := leaderOf(nodes)
		if l == nil {
			if time.Now().After(deadline) {
				t.Fatalf("no leader after fail-over")
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if _, ok := l.Propose(kv.SetCmd{Key: "foo", Value: "bar"}); ok {
			break // success
		}
	}
	waitApplyAll(t, nodes, 1)
}

// ---------------------------------------------------------
// follower crashes, later catches up
// ---------------------------------------------------------
func TestFollowerCatchUp(t *testing.T) {
	nodes, stop := buildCluster(t, 3)
	defer stop()
	time.Sleep(2 * time.Second)

	_, ldr := leaderOf(nodes)
	if ldr == nil {
		t.Fatalf("no leader")
	}

	fIdx, follower := firstFollower(nodes)
	
	// Get and save the follower's ApplyCh for proper draining
	applyCh := follower.ApplyCh()
	killNode(follower) // take follower down
	
	// Drain the channel to prevent leaks
	go func() {
		for range applyCh {
			// Drain until closed
		}
	}()

	// leader keeps working
	for i := 0; i < 10; i++ {
		_, _ = ldr.Propose(kv.SetCmd{Key: fmt.Sprintf("k%d", i), Value: "v"})
	}
	waitApplyAll(t, []*raft.Node{ldr}, 10)

	// follower returns and must catch up
	nodes[fIdx] = restartNode(t, nodes[fIdx])
	waitApplyAll(t, nodes, 10)
}

// ---------------------------------------------------------
// single-node restart tests persistence + leadership
// ---------------------------------------------------------
func TestLeaderRestartWithDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "solo.bolt")
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt DB: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg, 128)
	node := raft.NewNode("solo", nil, applyCh, db)
	go node.Start()

	// --- wait for self-election -----------------------
	waitLeader := func(n *raft.Node, d time.Duration) {
		deadline := time.Now().Add(d)
		for n.State() != raft.Leader {
			if time.Now().After(deadline) {
				t.Fatalf("node never became leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitLeader(node, 500*time.Millisecond)

	// --- now the proposal will be accepted ------------
	idx, ok := node.Propose(kv.SetCmd{Key: "x", Value: "y"})
	if !ok {
		t.Fatalf("propose rejected by leader")
	}

	for node.LastApplied() < idx {
		time.Sleep(10 * time.Millisecond)
	}
	
	// Properly drain the apply channel
	drainCh := applyCh
	node.Stop()
	
	go func() {
		for range drainCh {
			// Drain until closed
		}
	}()
	
	db.Close()

	// ---------- restart -------------------------------
	db2, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to reopen bolt DB: %v", err)
	}
	
	applyCh2 := make(chan raft.ApplyMsg, 128)
	node2 := raft.NewNode("solo", nil, applyCh2, db2)
	go node2.Start()

	waitLeader(node2, 500*time.Millisecond)

	if node2.LastApplied() < idx {
		t.Fatalf("persisted entry missing after restart")
	}
	
	node2.Stop()
	go func() {
		for range applyCh2 {
			// Drain until closed
		}
	}()
	
	db2.Close()
}

/*=========================================================
                helper functions
=========================================================*/

func leaderOf(ns []*raft.Node) (int, *raft.Node) {
	for i, n := range ns {
		if n == nil {
			continue
		} // skip nil nodes
		if n.State() == raft.Leader {
			return i, n
		}
	}
	return -1, nil
}
func firstFollower(ns []*raft.Node) (int, *raft.Node) {
	for i, n := range ns {
		if n.State() == raft.Follower {
			return i, n
		}
	}
	return -1, nil
}

func waitApplyAll(t *testing.T, ns []*raft.Node, want int) {
	t.Helper()
	dead := time.Now().Add(5 * time.Second)
	for _, n := range ns {
		if n == nil {
			continue // Skip nil nodes
		}
		for n.LastApplied() < want {
			if time.Now().After(dead) {
				t.Fatalf("node %s stuck at %d/%d",
					n.ID(), n.LastApplied(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func killNode(n *raft.Node) { 
	n.Stop() 
}

func restartNode(t *testing.T, old *raft.Node) *raft.Node {
	t.Helper()

	// Get DB and ID before operations that might make old unavailable
	db := old.GetDB()
	id := old.ID()
	peers := old.PeersCopy()
	
	// Save apply channel for draining
	oldCh := old.ApplyCh()
	
	// Stop the old node
	old.Stop()

	// Drain the old ApplyCh until the sender closes it
	go func(ch <-chan raft.ApplyMsg) {
		for range ch {
			// Drain until closed
		}
	}(oldCh)

	// Allow time for cleanup
	time.Sleep(100 * time.Millisecond)

	newCh := make(chan raft.ApplyMsg, 256)
	newN := raft.NewNode(id, peers, newCh, db)
	go newN.Start()
	return newN
}

// func restartAll(t *testing.T, ns []*raft.Node) {
// 	for i, n := range ns {
// 		ns[i] = restartNode(t, n)
// 	}
// }