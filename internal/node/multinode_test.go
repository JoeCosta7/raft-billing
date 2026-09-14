package node

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
	"testing"
	"time"
)

func durationPtr(d time.Duration) *time.Duration { return &d }

type testCluster struct {
	nodes []*Node
}

func newTestCluster(t *testing.T, n int) *testCluster {
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

	nodes := make([]*Node, n)
	for i := range n {
		cfg := &config.Config{
			NodeID:    nodeIDs[i],
			RaftAddr:  addrs[i],
			HTTPAddr:  freeAddr(t),
			DataDir:   t.TempDir(),
			Peers:     peers,
			Bootstrap: i == 0, // exactly one node bootstraps the full member list
		}
		node, err := New(cfg)
		if err != nil {
			t.Fatalf("New(%s): %v", nodeIDs[i], err)
		}
		nodes[i] = node
	}

	// Start every node's transport before any election can succeed — a
	// bootstrapped configuration naming 3 voters can't reach quorum if the
	// other two aren't listening yet.
	for _, n := range nodes {
		if err := n.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}

	c := &testCluster{nodes: nodes}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, n := range c.nodes {
			if n != nil {
				n.Shutdown(shutdownCtx)
			}
		}
	})
	return c
}

// waitForClusterLeader polls until exactly one of the given nodes reports
// itself leader, or fails the test.
func waitForClusterLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var leader *Node
		count := 0
		for _, n := range nodes {
			if n != nil && n.raftNode.IsLeader() {
				leader = n
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("cluster never converged on exactly one leader within the timeout")
	return nil
}

func withoutNode(nodes []*Node, exclude *Node) []*Node {
	var out []*Node
	for _, n := range nodes {
		if n != exclude {
			out = append(out, n)
		}
	}
	return out
}

func TestCluster_ElectsExactlyOneLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := waitForClusterLeader(t, c.nodes)

	followers := 0
	for _, n := range c.nodes {
		if n == leader {
			continue
		}
		if n.raftNode.IsLeader() {
			t.Errorf("node %s: reports leader alongside %s", n.raftNode.ID(), leader.raftNode.ID())
		}
		followers++
	}
	if followers != 2 {
		t.Errorf("followers: got %d, want 2", followers)
	}
}

// TestCluster_WriteReplicatesToAllNodes proves replication actually
// happens — every previous test that "confirms" a write always read it back
// through the same node that wrote it, which proves nothing about the other
// replicas. This checks each node's own storage independently.
func TestCluster_WriteReplicatesToAllNodes(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := waitForClusterLeader(t, c.nodes)

	if _, err := leader.raftNode.Propose("create_tenant", command.CreateTenantCommand{ID: "t1", Name: "Acme"}, 5*time.Second); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	for _, n := range c.nodes {
		deadline := time.Now().Add(10 * time.Second)
		var tenant *model.Tenant
		for {
			if err := n.stateMachine.Storage().View(func(tx storage.Tx) error {
				got, err := tx.GetTenant("t1")
				tenant = got
				return err
			}); err != nil {
				t.Fatalf("node %s: View: %v", n.raftNode.ID(), err)
			}
			if tenant != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %s: tenant t1 never replicated within the timeout", n.raftNode.ID())
			}
			time.Sleep(50 * time.Millisecond)
		}
		if tenant.Name != "Acme" {
			t.Errorf("node %s: tenant.Name: got %q, want %q", n.raftNode.ID(), tenant.Name, "Acme")
		}
	}
}

func TestCluster_LeaderFailover_NewLeaderElectedAndServesWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeader := waitForClusterLeader(t, c.nodes)
	oldLeaderID := oldLeader.raftNode.ID()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := oldLeader.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown old leader: %v", err)
	}

	remaining := withoutNode(c.nodes, oldLeader)
	newLeader := waitForClusterLeader(t, remaining)
	if newLeader.raftNode.ID() == oldLeaderID {
		t.Fatalf("expected a different node to take over, got the same ID %q", oldLeaderID)
	}

	if _, err := newLeader.raftNode.Propose("create_tenant", command.CreateTenantCommand{ID: "t2", Name: "Post-failover"}, 5*time.Second); err != nil {
		t.Fatalf("Propose after failover: %v", err)
	}
	tenant, err := newLeader.raftNode.GetTenant("t2")
	if err != nil {
		t.Fatalf("GetTenant after failover: %v", err)
	}
	if tenant == nil {
		t.Fatal("tenant not found after a post-failover write")
	}
}

// TestCluster_Failover_RecoversOrphanedInFlightExecution is the one this
// whole exercise was really about: does adoptOrComplete/runRecover — the
// machinery this session spent the most effort hardening — actually work
// against a real replicated cluster, not just the fakes it's been tested
// against everywhere else? It builds the exact scenario that logic exists
// for: a node claims and starts dispatching an execution, then dies before
// recording any outcome, and the new leader has to notice and adopt it.
func TestCluster_Failover_RecoversOrphanedInFlightExecution(t *testing.T) {
	// The callback never responds, so whichever node dispatches this
	// execution can never get far enough to record an attempt before we
	// kill it out from under the in-flight HTTP call.
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer server.Close()
	defer close(block) // must unblock before server.Close() can return; see defer order below

	c := newTestCluster(t, 3)
	leader1 := waitForClusterLeader(t, c.nodes)

	if _, err := leader1.raftNode.Propose("create_tenant", command.CreateTenantCommand{ID: "t1", Name: "Acme"}, 5*time.Second); err != nil {
		t.Fatalf("propose create_tenant: %v", err)
	}
	createSchedule := command.CreateScheduleCommand{
		ID:           "s1",
		TenantID:     "t1",
		CallbackURL:  server.URL,
		Payload:      []byte("{}"),
		ScheduleType: model.ScheduleTypeOnce,
		FirstRunAt:   time.Now().Add(-time.Second), // already due
		Timezone:     "UTC",
		MaxAttempts:  3,
		RetryBackoff: model.RetryBackoff{Initial: 100 * time.Millisecond, Multiplier: 2},
		CallTimeout:  durationPtr(time.Minute), // long enough it won't fire before we kill leader1
	}
	if _, err := leader1.raftNode.Propose("create_schedule", createSchedule, 5*time.Second); err != nil {
		t.Fatalf("propose create_schedule: %v", err)
	}

	// Wait for leader1's real Worker (via its real steady-state tick, not
	// anything triggered manually) to actually claim it.
	var execID string
	deadline := time.Now().Add(15 * time.Second)
	for {
		execs, err := leader1.raftNode.ListExecutionsByStatus("t1", model.ExecutionStatusInFlight)
		if err != nil {
			t.Fatalf("ListExecutionsByStatus: %v", err)
		}
		if len(execs) == 1 {
			if execs[0].OwnerNodeID != leader1.raftNode.ID() {
				t.Fatalf("exec.OwnerNodeID: got %q, want %q", execs[0].OwnerNodeID, leader1.raftNode.ID())
			}
			if execs[0].AttemptCount != 0 {
				t.Fatalf("exec.AttemptCount: got %d, want 0 (must be orphaned before any attempt is recorded)", execs[0].AttemptCount)
			}
			execID = execs[0].ID
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("execution was never claimed within the timeout")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Kill leader1 now, mid-dispatch — the exact orphaned-execution scenario
	// adoptOrComplete/runRecover exist for.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := leader1.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown leader1: %v", err)
	}

	leader2 := waitForClusterLeader(t, withoutNode(c.nodes, leader1))

	// leader2's runRecover (via Scheduler.Start -> Supervisor -> Worker.Run)
	// must adopt this orphaned execution for real, against real replicated
	// storage — not the fakes every other test of this logic uses.
	deadline = time.Now().Add(15 * time.Second)
	for {
		got, err := leader2.raftNode.GetExecution("t1", execID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if got.OwnerNodeID == leader2.raftNode.ID() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution was never adopted by the new leader within the timeout (owner still %q)", got.OwnerNodeID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
