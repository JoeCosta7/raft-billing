package raftnode

import (
	"context"
	"net"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/internal/statemachine"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// newTestRaftNode builds a single-node RaftNode against real temp-dir
// storage. bootstrap=false deliberately leaves it with no configuration at
// all, so it sits in Follower state forever — the only deterministic way to
// exercise the "not leader" branch of every read method without a multi-node
// cluster.
func newTestRaftNode(t *testing.T, bootstrap bool) *RaftNode {
	t.Helper()
	addr := freeAddr(t)
	cfg := &config.Config{
		NodeID:    "node1",
		RaftAddr:  addr,
		DataDir:   t.TempDir(),
		Peers:     map[string]string{"node1": addr},
		Bootstrap: bootstrap,
	}
	sm, err := statemachine.New(cfg)
	if err != nil {
		t.Fatalf("statemachine.New: %v", err)
	}
	t.Cleanup(func() {
		if err := sm.Shutdown(context.Background()); err != nil {
			t.Errorf("cleanup statemachine.Shutdown: %v", err)
		}
	})
	rn, err := New(cfg, sm.Storage(), sm)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := rn.Shutdown(context.Background()); err != nil {
			t.Errorf("cleanup RaftNode.Shutdown: %v", err)
		}
	})
	return rn
}

func waitForLeader(t *testing.T, rn *RaftNode) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rn.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("node never became leader within the timeout")
}

func TestRaftNode_ID(t *testing.T) {
	rn := newTestRaftNode(t, true)
	if rn.ID() != "node1" {
		t.Errorf("ID: got %q, want %q", rn.ID(), "node1")
	}
}

// TestRaftNode_NeverBootstrapped_NonLeaderReadsAllError proves every
// leader-gated method actually rejects a non-leader call — none of these
// branches had ever been exercised by any test before this, since every
// prior RaftNode-backed test bootstraps a single node, which is always
// leader immediately.
func TestRaftNode_NeverBootstrapped_NonLeaderReadsAllError(t *testing.T) {
	rn := newTestRaftNode(t, false)

	// No configuration was ever bootstrapped, so this node cannot hold an
	// election and will sit in Follower state indefinitely — no need to
	// poll/wait, it's deterministically never going to become leader.
	if rn.IsLeader() {
		t.Fatal("expected a never-bootstrapped node to never be leader")
	}

	if _, err := rn.ListTenants(); err == nil {
		t.Error("ListTenants: expected an error when not leader, got nil")
	}
	if _, err := rn.GetTenant("t1"); err == nil {
		t.Error("GetTenant: expected an error when not leader, got nil")
	}
	if _, err := rn.GetSchedule("t1", "s1"); err == nil {
		t.Error("GetSchedule: expected an error when not leader, got nil")
	}
	if _, err := rn.ListSchedulesDue("t1"); err == nil {
		t.Error("ListSchedulesDue: expected an error when not leader, got nil")
	}
	if _, err := rn.GetExecution("t1", "e1"); err == nil {
		t.Error("GetExecution: expected an error when not leader, got nil")
	}
	if _, err := rn.GetAttempt("t1", "a1"); err == nil {
		t.Error("GetAttempt: expected an error when not leader, got nil")
	}
	if _, err := rn.ListExecutionsBySchedule("t1", "s1"); err == nil {
		t.Error("ListExecutionsBySchedule: expected an error when not leader, got nil")
	}
	if _, err := rn.ListExecutionsByStatus("t1", model.ExecutionStatusInFlight); err == nil {
		t.Error("ListExecutionsByStatus: expected an error when not leader, got nil")
	}
	if _, err := rn.ListAttemptsByExecution("t1", "e1"); err == nil {
		t.Error("ListAttemptsByExecution: expected an error when not leader, got nil")
	}
	if _, err := rn.Propose("create_tenant", command.CreateTenantCommand{ID: "t1"}, time.Second); err == nil {
		t.Error("Propose: expected an error when not leader, got nil")
	}
}

func TestRaftNode_SingleNodeBootstrap_BecomesLeaderAndServesReads(t *testing.T) {
	rn := newTestRaftNode(t, true)
	waitForLeader(t, rn)

	tenants, err := rn.ListTenants()
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	if len(tenants) != 0 {
		t.Errorf("ListTenants: got %d, want 0 on a fresh cluster", len(tenants))
	}
}

func TestRaftNode_LeadershipCh_SignalsTrueOnBecomingLeader(t *testing.T) {
	rn := newTestRaftNode(t, true)
	select {
	case isLeader := <-rn.LeadershipCh():
		if !isLeader {
			t.Error("LeadershipCh: got false, want true")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a leadership notification")
	}
}

func TestRaftNode_Propose_AppliesAndIsReadable(t *testing.T) {
	rn := newTestRaftNode(t, true)
	waitForLeader(t, rn)

	if _, err := rn.Propose("create_tenant", command.CreateTenantCommand{ID: "t1", Name: "Acme", APIKeyHash: "test-hash"}, 5*time.Second); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	tenant, err := rn.GetTenant("t1")
	if err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if tenant == nil || tenant.ID != "t1" {
		t.Errorf("GetTenant: got %+v, want tenant t1", tenant)
	}
}

func TestRaftNode_Shutdown_NoError(t *testing.T) {
	addr := freeAddr(t)
	cfg := &config.Config{
		NodeID:    "node1",
		RaftAddr:  addr,
		DataDir:   t.TempDir(),
		Peers:     map[string]string{"node1": addr},
		Bootstrap: true,
	}
	sm, err := statemachine.New(cfg)
	if err != nil {
		t.Fatalf("statemachine.New: %v", err)
	}
	defer sm.Shutdown(context.Background())
	rn, err := New(cfg, sm.Storage(), sm)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	waitForLeader(t, rn)

	if err := rn.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: unexpected error: %v", err)
	}
}
