package tests

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

func pickNPorts(n int) ([]int, error) {
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}
		addr, ok := ln.Addr().(*net.TCPAddr)
		if !ok {
			ln.Close()
			return nil, fmt.Errorf("expected TCPAddr, got %T", ln.Addr())
		}
		port := addr.Port
		ln.Close()
		ports = append(ports, port)
	}
	return ports, nil
}

// buildCluster spins up N raft nodes each with its own BoltDB file in a temp dir.
func buildCluster(t *testing.T, n int) ([]*raft.Node, func()) {
	t.Helper()

	// Prepare resource tracking for cleanup
	var resources struct {
		nodes []*raft.Node
		dbs   []*bolt.DB
		httpServers []*http.Server
	}
	
	// Define cleanup function that can be called at any point
	cleanup := func() {
		// Stop all HTTP servers
		for _, server := range resources.httpServers {
			if server != nil {
				// Use a context with timeout if in production code
				server.Close()
			}
		}
		
		// Stop all nodes
		for _, n := range resources.nodes {
			if n != nil {
				n.Stop()
			}
		}
		
		// Close all DBs
		for _, db := range resources.dbs {
			if db != nil {
				db.Close()
			}
		}
	}

	ports, err := pickNPorts(n)
	if err != nil {
		t.Fatalf("port pick: %v", err)
	}

	// pick free ports
	addrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		addrs[fmt.Sprintf("node%d", i+1)] = fmt.Sprintf("localhost:%d", ports[i])
	}

	nodes := make([]*raft.Node, n)
	
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr := addrs[id]
		
		// peer map ----------------------------------------------------
		peers := make(map[string]string)
		for pid, paddr := range addrs {
			if pid != id {
				peers[pid] = paddr
			}
		}

		// bolt db -----------------------------------------------------
		dbPath := filepath.Join(t.TempDir(), id+".bolt")
		db, err := bolt.Open(dbPath, 0600, nil)
		if err != nil {
			cleanup() // Clean up resources allocated so far
			t.Fatalf("open bolt: %v", err)
		}
		resources.dbs = append(resources.dbs, db)

		// raft node ---------------------------------------------------
		applyCh := make(chan raft.ApplyMsg, 128)
		node := raft.NewNode(id, peers, applyCh, db)
		nodes[i] = node
		resources.nodes = append(resources.nodes, node)

		// drain applyCh so it never blocks ---------------------------
		go func(ch <-chan raft.ApplyMsg) {
			for range ch { /* discard */
			}
		}(applyCh)

		// start HTTP listener with proper error handling -------------
		go func(n *raft.Node, addr string) {
			n.Start()
			mux := http.NewServeMux()
			mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))
			
			server := &http.Server{
				Addr:    addr,
				Handler: mux,
			}
			resources.httpServers = append(resources.httpServers, server)
			
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("HTTP server for node %s failed: %v", n.ID(), err)
			}
		}(node, addr)
	}

	return nodes, cleanup
}

// --------------------------------------------------
// Leader election + Replication
// --------------------------------------------------
func TestEndToEndReplication(t *testing.T) {
	nodes, stop := buildCluster(t, 5)
	defer stop()

	time.Sleep(4 * time.Second) // allow election
	var leader *raft.Node
	for _, n := range nodes {
		if n != nil && n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	idx, ok := leader.Propose(kv.SetCmd{Key: "foo", Value: "bar"})
	if !ok {
		t.Fatalf("propose failed")
	}

	// wait for all nodes to apply the entry
	for leader.LastApplied() < idx {
		time.Sleep(10 * time.Millisecond)
	}

	deadline := time.Now().Add(10 * time.Second)
	for _, n := range nodes {
		if n == nil {
			continue
		}
		id := n.ID()
		for n.LastApplied() < idx {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not apply idx=%d", id, idx)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// --------------------------------------------------
// Concurrency: many writes in parallel
// --------------------------------------------------
func TestConcurrentWrites(t *testing.T) {
	nodes, stop := buildCluster(t, 1)
	defer stop()
	time.Sleep(2 * time.Second)

	var leader *raft.Node
	for _, n := range nodes {
		if n != nil && n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	entry_count := 1000
	var errMutex sync.Mutex
	var errors []string

	var wg sync.WaitGroup
	for i := 0; i < entry_count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, ok := leader.Propose(kv.SetCmd{Key: fmt.Sprintf("k%02d", i), Value: "x"}); !ok {
				errMutex.Lock()
				errors = append(errors, fmt.Sprintf("propose %d failed", i))
				errMutex.Unlock()
			}
		}(i)
		// Small delay to prevent overwhelming the system
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()
	
	// Report collected errors
	for _, err := range errors {
		t.Errorf("%s", err)
	}

	// wait for all nodes to apply the entries
	deadline := time.Now().Add(20 * time.Second)
	for _, n := range nodes {
		if n == nil {
			continue
		}
		id := n.ID()
		for n.LastApplied() < entry_count {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not apply all entries, applied %d/%d", 
					id, n.LastApplied(), entry_count)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}