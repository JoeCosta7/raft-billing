package node

import (
	"bytes"
	"context"
	"encoding/json"
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
		HTTPAddr:  freeAddr(t), // exercised directly in TestNode_SingleNodeBootstrap_HTTPCreatesAndDispatchesSchedule below; this test drives the worker path directly via Propose instead
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

	result, err := n.raftNode.Propose("create_tenant", command.CreateTenantCommand{
		ID:         "tenant-1",
		Name:       "Test Tenant",
		APIKeyHash: "test-hash",
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("propose create_tenant: %v", err)
	}
	if cmdErr, ok := result.(*command.CommandError); ok {
		t.Fatalf("create_tenant rejected: %v", cmdErr)
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

// TestNode_SingleNodeBootstrap_HTTPCreatesAndDispatchesSchedule is the
// Phase-3 companion to the test above: instead of calling rn.Propose
// directly, it drives everything through real HTTP requests against
// internal/api — the one path the previous test deliberately bypassed —
// proving a client outside the process can actually create a tenant and
// schedule and see it dispatch for real.
func TestNode_SingleNodeBootstrap_HTTPCreatesAndDispatchesSchedule(t *testing.T) {
	var hits int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	const nodeID = "node1"
	raftAddr := freeAddr(t)
	httpAddr := freeAddr(t)
	cfg := &config.Config{
		NodeID:    nodeID,
		RaftAddr:  raftAddr,
		HTTPAddr:  httpAddr,
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
	base := "http://" + httpAddr

	httpJSON(t, http.MethodPost, base+"/tenants", command.CreateTenantCommand{
		ID:   "tenant-1",
		Name: "Test Tenant",
	}, http.StatusCreated)

	createSchedule := command.CreateScheduleCommand{
		ID:           "sched-1",
		TenantID:     "tenant-1",
		CallbackURL:  callbackServer.URL,
		Payload:      []byte("{}"),
		ScheduleType: model.ScheduleTypeOnce,
		FirstRunAt:   time.Now().Add(-time.Second),
		Timezone:     "UTC",
		MaxAttempts:  3,
		RetryBackoff: model.RetryBackoff{Initial: 100 * time.Millisecond, Multiplier: 2},
	}
	httpJSON(t, http.MethodPost, base+"/tenants/tenant-1/schedules", createSchedule, http.StatusCreated)

	// Confirm the read path too, not just the write.
	getResp := httpJSON(t, http.MethodGet, base+"/tenants/tenant-1/schedules/sched-1", nil, http.StatusOK)
	var got model.Schedule
	if err := json.Unmarshal(getResp, &got); err != nil {
		t.Fatalf("decode GET schedule response: %v", err)
	}
	if got.ID != "sched-1" || got.CallbackURL != callbackServer.URL {
		t.Errorf("GET schedule: got %+v", got)
	}

	deadline := time.Now().Add(15 * time.Second)
	for atomic.LoadInt32(&hits) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&hits); got == 0 {
		t.Fatal("callback was never invoked within the timeout — schedule created over HTTP never dispatched")
	}
}

// httpJSON does a JSON request against the real HTTP server, fails the test
// if the status code doesn't match wantStatus, and returns the response body.
func httpJSON(t *testing.T, method, url string, body any, wantStatus int) []byte {
	t.Helper()
	var reqBody *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reqBody = bytes.NewReader(b)
	} else {
		reqBody = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	respBody := new(bytes.Buffer)
	if _, err := respBody.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status: got %d, want %d, body: %s", method, url, resp.StatusCode, wantStatus, respBody.String())
	}
	return respBody.Bytes()
}
