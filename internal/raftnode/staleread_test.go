package raftnode

import (
	"context"
	"fmt"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/statemachine"
	"testing"
	"time"
)

// TestEnsureCaughtUpAsLeader_CachesPerTerm is a fast, deterministic check of
// the caching property (one Barrier call per leadership term, not one per
// read) — white-box, reading the unexported readyTerm field directly, so it
// doesn't depend on timing the way the failover test below necessarily does.
func TestEnsureCaughtUpAsLeader_CachesPerTerm(t *testing.T) {
	rn := newTestRaftNode(t, true)
	waitForLeader(t, rn)

	if rn.readyTerm != 0 {
		t.Fatalf("readyTerm before first call: got %d, want 0", rn.readyTerm)
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		t.Fatalf("first call: %v", err)
	}
	term := rn.raft.CurrentTerm()
	if rn.readyTerm != term {
		t.Fatalf("readyTerm after first call: got %d, want current term %d", rn.readyTerm, term)
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if rn.readyTerm != term {
		t.Fatalf("readyTerm after second call: got %d, want unchanged %d", rn.readyTerm, term)
	}
}

// newRawTestCluster builds n RaftNode+StateMachine pairs directly (no
// Scheduler/Node/API layer at all) so a test can exercise RaftNode's read
// path in isolation, uncontaminated by anything else that might separately
// call Barrier.
func newRawTestCluster(t *testing.T, n int) []*RaftNode {
	t.Helper()
	nodeIDs := make([]string, n)
	addrs := make([]string, n)
	for i := range n {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
		addrs[i] = freeAddr(t)
	}
	peers := make(map[string]string, n)
	for i := range n {
		peers[nodeIDs[i]] = addrs[i]
	}

	rns := make([]*RaftNode, n)
	for i := range n {
		cfg := &config.Config{
			NodeID:    nodeIDs[i],
			RaftAddr:  addrs[i],
			DataDir:   t.TempDir(),
			Peers:     peers,
			Bootstrap: i == 0,
		}
		sm, err := statemachine.New(cfg)
		if err != nil {
			t.Fatalf("statemachine.New(%s): %v", nodeIDs[i], err)
		}
		t.Cleanup(func() {
			if err := sm.Shutdown(context.Background()); err != nil {
				t.Errorf("cleanup statemachine.Shutdown(%s): %v", nodeIDs[i], err)
			}
		})
		rn, err := New(cfg, sm.Storage(), sm)
		if err != nil {
			t.Fatalf("New(%s): %v", nodeIDs[i], err)
		}
		t.Cleanup(func() {
			// Already-shut-down nodes (the killed leader in the failover
			// test) return an error here on the second Shutdown — expected,
			// not asserted on.
			rn.Shutdown(context.Background())
		})
		rns[i] = rn
	}
	return rns
}

func waitForAnyLeader(t *testing.T, rns []*RaftNode, exclude *RaftNode) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, rn := range rns {
			if rn != exclude && rn.IsLeader() {
				return rn
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within the timeout")
	return nil
}

// TestCluster_NewLeaderReadsNeverStaleAfterFailover is the permanent
// regression guard for the bug found via real multi-node testing: before
// RaftNode.ensureCaughtUpAsLeader existed, a newly-elected leader's public
// read methods (GetTenant here) missed data committed under the old leader
// in 77-83% of trials across repeated runs — this was the default outcome
// of a failover, not a rare edge case. 10 trials is already overwhelming
// statistical power against that failure rate (the odds of 10/10 passing by
// chance at an 80% underlying failure rate are on the order of 1e-7); it
// doesn't need the 30 used while first characterizing the bug.
func TestCluster_NewLeaderReadsNeverStaleAfterFailover(t *testing.T) {
	const trials = 10
	for attempt := range trials {
		t.Run(fmt.Sprintf("attempt%d", attempt), func(t *testing.T) {
			rns := newRawTestCluster(t, 3)
			leader := waitForAnyLeader(t, rns, nil)

			var lastID string
			for i := range 20 {
				lastID = fmt.Sprintf("t%d", i)
				if _, err := leader.Propose("create_tenant", command.CreateTenantCommand{ID: lastID, Name: "x"}, 5*time.Second); err != nil {
					t.Fatalf("Propose: %v", err)
				}
			}

			if err := leader.Shutdown(context.Background()); err != nil {
				t.Fatalf("Shutdown leader: %v", err)
			}

			newLeader := waitForAnyLeader(t, rns, leader)

			got, err := newLeader.GetTenant(lastID)
			if err != nil {
				t.Fatalf("GetTenant on new leader: %v", err)
			}
			if got == nil {
				t.Fatalf("new leader's GetTenant missed %q, committed under the old leader before failover", lastID)
			}
		})
	}
}
