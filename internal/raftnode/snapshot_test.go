package raftnode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/statemachine"
	"raft-biling/internal/storage"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// syncBuffer lets a test safely read raft's log output from a different
// goroutine than the one writing it (raft logs from its own internal
// goroutines).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Contains(s string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Contains(b.buf.String(), s)
}

// snapshotTunedNode mirrors raftnode.New's wiring exactly, except the
// snapshot thresholds are tuned aggressively (production always uses
// raft.DefaultConfig(), which needs thousands of log entries before a
// snapshot) so this test can force a real snapshot without an impractically
// long write burst, and its own logger is captured so the test can look for
// direct proof of what raft actually did, not just infer it from end state.
// Production code isn't touched — raftnode.go has no hook for this, and
// adding test-only config surface there for one test's benefit isn't worth
// it, so this duplicates the wiring instead.
type snapshotTunedNode struct {
	raft          *raft.Raft
	sm            *statemachine.StateMachine
	snapshotStore *raft.FileSnapshotStore
	logStore      *raftboltdb.BoltStore
	logs          *syncBuffer
}

func newSnapshotTunedNode(t *testing.T, cfg *config.Config) *snapshotTunedNode {
	t.Helper()
	sm, err := statemachine.New(cfg)
	if err != nil {
		t.Fatalf("statemachine.New(%s): %v", cfg.NodeID, err)
	}
	t.Cleanup(func() { sm.Shutdown(context.Background()) })

	logs := &syncBuffer{}
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.SnapshotThreshold = 10
	rc.SnapshotInterval = 100 * time.Millisecond
	rc.TrailingLogs = 5
	rc.LogOutput = logs

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		t.Fatalf("logStore(%s): %v", cfg.NodeID, err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		t.Fatalf("stableStore(%s): %v", cfg.NodeID, err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		t.Fatalf("snapshotStore(%s): %v", cfg.NodeID, err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, time.Second, os.Stderr)
	if err != nil {
		t.Fatalf("transport(%s): %v", cfg.NodeID, err)
	}
	r, err := raft.NewRaft(rc, sm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		t.Fatalf("NewRaft(%s): %v", cfg.NodeID, err)
	}
	t.Cleanup(func() {
		r.Shutdown()
		logStore.Close()
		stableStore.Close()
		transport.Close()
	})
	return &snapshotTunedNode{raft: r, sm: sm, snapshotStore: snapshotStore, logStore: logStore, logs: logs}
}

func waitForSnapshotTunedLeader(t *testing.T, nodes []*snapshotTunedNode) *snapshotTunedNode {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within the timeout")
	return nil
}

// proposeRaw applies a command directly through *raft.Raft — this test
// works one layer below RaftNode.Propose, since it needs a raft.Config with
// tuned snapshot thresholds that raftnode.New has no way to accept.
func proposeRaw(t *testing.T, r *raft.Raft, cmdType string, cmd any) {
	t.Helper()
	payload, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	data, err := json.Marshal(command.LogEntry{Type: cmdType, ProposedAt: time.Now(), Payload: payload})
	if err != nil {
		t.Fatalf("marshal log entry: %v", err)
	}
	future := r.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		t.Fatalf("Apply(%s): %v", cmdType, err)
	}
	// future.Error() only reports transport/commit failures — a command
	// the FSM rejected (e.g. failed validation) still commits and applies
	// cleanly, returning a *command.CommandError as the response instead.
	// Checking only Error() here previously let every create_tenant in
	// this test silently no-op once api_key_hash became required, and the
	// test kept passing for the wrong reason (the write-burst's index
	// still advanced) until the final assertion failed confusingly.
	if cmdErr, ok := future.Response().(*command.CommandError); ok {
		t.Fatalf("Apply(%s) rejected: %v", cmdType, cmdErr)
	}
}

// TestSnapshot_LateJoiningNodeCatchesUpViaRealInstallSnapshot exercises the
// path nothing else in this codebase has ever driven: Raft's own automatic
// snapshot-and-InstallSnapshot mechanism, not a directly-called
// Snapshot()/Restore() round trip (that's already covered by
// statemachine_test.go's TestSnapshotAndRestore_RoundTrip). A node that
// joins with zero log entries after the leader has already snapshotted and
// trimmed its log can only catch up one way — a real InstallSnapshot RPC —
// which is what forces the scenario deterministically rather than hoping to
// catch a timing window.
func TestSnapshot_LateJoiningNodeCatchesUpViaRealInstallSnapshot(t *testing.T) {
	addr1, addr2, addr3 := freeAddr(t), freeAddr(t), freeAddr(t)

	node1 := newSnapshotTunedNode(t, &config.Config{NodeID: "node1", RaftAddr: addr1, DataDir: t.TempDir()})
	node2 := newSnapshotTunedNode(t, &config.Config{NodeID: "node2", RaftAddr: addr2, DataDir: t.TempDir()})

	// All three are bootstrapped as voters from the start — node3's own
	// *raft.Raft just isn't constructed yet. This deliberately avoids
	// AddVoter/dynamic membership change as a variable (an earlier version
	// of this test used it and hung for 900-8000s at least twice across a
	// handful of runs, cause unconfirmed): node3 rejoins as an
	// already-known peer with empty local state, the same way a real node
	// that was offline and comes back up would, rather than being added to
	// the cluster fresh via a config-change RPC.
	if fut := node1.raft.BootstrapCluster(raft.Configuration{Servers: []raft.Server{
		{ID: "node1", Address: raft.ServerAddress(addr1), Suffrage: raft.Voter},
		{ID: "node2", Address: raft.ServerAddress(addr2), Suffrage: raft.Voter},
		{ID: "node3", Address: raft.ServerAddress(addr3), Suffrage: raft.Voter},
	}}); fut.Error() != nil {
		t.Fatalf("BootstrapCluster: %v", fut.Error())
	}

	leader := waitForSnapshotTunedLeader(t, []*snapshotTunedNode{node1, node2})

	const numTenants = 25 // comfortably past SnapshotThreshold=10
	tenantIDs := make([]string, numTenants)
	for i := range numTenants {
		id := fmt.Sprintf("t%d", i)
		tenantIDs[i] = id
		proposeRaw(t, leader.raft, "create_tenant", command.CreateTenantCommand{ID: id, Name: "x", APIKeyHash: "test-hash"})
	}

	// Confirm the leader has not just taken a snapshot but actually
	// physically truncated its log past index 1 — a snapshot file existing
	// and the old log entries actually being deleted from logStore are two
	// different moments, and checking only the former was flaky (the
	// leader could still have index 1 on disk, in which case raft
	// legitimately chooses ordinary replay over InstallSnapshot — correct
	// raft behavior, just not the scenario this test needs to force).
	// FirstIndex() reflects the log store's physical state directly, so
	// waiting for it to move past 1 is the real signal, not a proxy for it.
	deadline := time.Now().Add(10 * time.Second)
	for {
		first, err := leader.logStore.FirstIndex()
		if err != nil {
			t.Fatalf("logStore.FirstIndex: %v", err)
		}
		if first > 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader's log was never truncated past index 1 within the timeout (FirstIndex=%d)", first)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The target index node3 needs to reach — captured now, before it even
	// starts, from the leader's own already-committed state.
	targetIndex := leader.raft.LastIndex()

	// Only now does node3 actually start — already a known voter in the
	// cluster's committed configuration (from the bootstrap above), but
	// with zero log entries of its own on disk. The leader has no way to
	// catch it up except a real InstallSnapshot.
	node3 := newSnapshotTunedNode(t, &config.Config{NodeID: "node3", RaftAddr: addr3, DataDir: t.TempDir()})

	// Wait using AppliedIndex — part of *raft.Raft's public, concurrency-
	// safe API — rather than polling node3.sm.Storage() directly in a tight
	// loop. StateMachine.storage/db are plain unsynchronized fields that
	// Restore() reassigns with no locking; an earlier version of this test
	// read them concurrently with an in-progress Restore and hit
	// "database not open" (and is the likely real explanation for this
	// test's earlier multi-hour hangs — a View() landing mid-close on a
	// *bolt.DB, not a plain race that fails cleanly). Nothing in production
	// does this: RaftNode's read methods only ever read a *leader's*
	// storage, and a node running Restore is by definition not currently
	// serving as leader, so this hazard is specific to reaching into a
	// follower's storage directly for test verification, not a real
	// concurrency bug in StateMachine as production actually uses it.
	deadline = time.Now().Add(20 * time.Second)
	for node3.raft.AppliedIndex() < targetIndex {
		if time.Now().After(deadline) {
			t.Fatalf("node3 never caught up (AppliedIndex=%d, want >= %d)", node3.raft.AppliedIndex(), targetIndex)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Safe to read now: node3 has reported itself caught up via raft's own
	// accounting, so any Restore that was going to happen has finished.
	for _, id := range tenantIDs {
		var found bool
		if err := node3.sm.Storage().View(func(tx storage.Tx) error {
			got, err := tx.GetTenant(id)
			found = got != nil
			return err
		}); err != nil {
			t.Fatalf("node3 View: %v", err)
		}
		if !found {
			t.Errorf("node3 caught up per AppliedIndex but is missing tenant %q", id)
		}
	}

	// Direct evidence, not just inferred from correct end state: raft's own
	// library logs this exact line when it successfully installs a
	// snapshot received from the leader.
	if !node3.logs.Contains("Installed remote snapshot") {
		t.Error(`node3's log never contained "Installed remote snapshot" — catch-up may have happened via ordinary log replay instead of a real InstallSnapshot, which is specifically what this test is supposed to force and verify`)
	}
}
