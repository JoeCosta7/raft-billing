package node

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"sync/atomic"
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

func waitForLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n.raftNode.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("node never became leader within the timeout")
}

func TestNode_SingleNodeBootstrap_DispatchesRealSchedule(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	const nodeID = "node1"
	raftAddr := freeAddr(t)
	cfg := &config.Config{
		NodeID:    nodeID,
		RaftAddr:  raftAddr,
		HTTPAddr:  "127.0.0.1:0", // unused today — internal/api is still a stub
		DataDir:   t.TempDir(),
		Peers:     map[string]string{nodeID: raftAddr},
		Bootstrap: true,
	}

	n, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := n.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := n.Shutdown(shutdownCtx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	waitForLeader(t, n)

	if _, err := n.raftNode.Propose("create_tenant", command.CreateTenantCommand{
		ID:   "tenant-1",
		Name: "Test Tenant",
	}, 5*time.Second); err != nil {
		t.Fatalf("propose create_tenant: %v", err)
	}

	createSchedule := command.CreateScheduleCommand{
		ID:           "sched-1",
		TenantID:     "tenant-1",
		CallbackURL:  server.URL,
		Payload:      []byte("{}"),
		ScheduleType: model.ScheduleTypeOnce,
		FirstRunAt:   time.Now().Add(-time.Second), // already due as of creation
		Timezone:     "UTC",
		MaxAttempts:  3,
		RetryBackoff: model.RetryBackoff{Initial: 100 * time.Millisecond, Multiplier: 2},
	}
	if _, err := n.raftNode.Propose("create_schedule", createSchedule, 5*time.Second); err != nil {
		t.Fatalf("propose create_schedule: %v", err)
	}

	// The worker's steady-state ticks every defaultTickInterval (5s today),
	// so this waits out at least one real tick — an integration test, not a
	// unit test, and allowed to be slower.
	deadline := time.Now().Add(15 * time.Second)
	for atomic.LoadInt32(&hits) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&hits); got == 0 {
		t.Fatal("callback was never invoked within the timeout — end-to-end dispatch did not fire")
	}
}
