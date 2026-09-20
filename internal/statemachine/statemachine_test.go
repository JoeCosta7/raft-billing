package statemachine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func newTestStateMachine(t *testing.T) *StateMachine {
	t.Helper()
	sm, err := New(&config.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := sm.Shutdown(context.Background()); err != nil {
			t.Errorf("cleanup Shutdown: %v", err)
		}
	})
	return sm
}

func logEntry(t *testing.T, cmdType string, payload any, proposedAt time.Time) *raft.Log {
	t.Helper()
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	data, err := json.Marshal(command.LogEntry{Type: cmdType, ProposedAt: proposedAt, Payload: payloadBytes})
	if err != nil {
		t.Fatalf("marshal log entry: %v", err)
	}
	return &raft.Log{Data: data}
}

func getTenant(t *testing.T, sm *StateMachine, id string) *model.Tenant {
	t.Helper()
	var tenant *model.Tenant
	if err := sm.Storage().View(func(tx storage.Tx) error {
		got, err := tx.GetTenant(id)
		tenant = got
		return err
	}); err != nil {
		t.Fatalf("View GetTenant(%q): %v", id, err)
	}
	return tenant
}

func TestApply_ValidCommand_WritesToStorage(t *testing.T) {
	sm := newTestStateMachine(t)
	result := sm.Apply(logEntry(t, "create_tenant", command.CreateTenantCommand{ID: "t1", Name: "Acme", APIKeyHash: "test-hash"}, time.Now()))

	tenant, ok := result.(*model.Tenant)
	if !ok {
		t.Fatalf("result: got %T (%v), want *model.Tenant", result, result)
	}
	if tenant.ID != "t1" {
		t.Errorf("tenant.ID: got %q, want %q", tenant.ID, "t1")
	}

	// Confirm it actually landed in storage, not just in Apply's return value.
	if got := getTenant(t, sm, "t1"); got == nil || got.ID != "t1" {
		t.Errorf("storage after Apply: got %+v, want tenant t1", got)
	}
}

func TestApply_CommandError_ReturnsErrorNotPanic(t *testing.T) {
	sm := newTestStateMachine(t)
	// Missing ID triggers ApplyCreateTenant's KindValidation CommandError.
	result := sm.Apply(logEntry(t, "create_tenant", command.CreateTenantCommand{Name: "no id"}, time.Now()))

	cmdErr, ok := result.(*command.CommandError)
	if !ok {
		t.Fatalf("result: got %T (%v), want *command.CommandError", result, result)
	}
	if cmdErr.Kind != command.KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, command.KindValidation)
	}
}

func TestApply_UnknownCmdType_Panics(t *testing.T) {
	sm := newTestStateMachine(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Apply to panic on an unknown command type, it didn't")
		}
	}()
	sm.Apply(logEntry(t, "not_a_real_command", struct{}{}, time.Now()))
}

func TestApply_MalformedEnvelope_Panics(t *testing.T) {
	sm := newTestStateMachine(t)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Apply to panic on a malformed log entry, it didn't")
		}
	}()
	sm.Apply(&raft.Log{Data: []byte("not json")})
}

func TestShutdown_ClosesStorage(t *testing.T) {
	sm, err := New(&config.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := sm.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: unexpected error: %v", err)
	}
	err = sm.Storage().View(func(tx storage.Tx) error { return nil })
	if err == nil {
		t.Fatal("expected storage access after Shutdown to fail (handle should be closed), got nil error")
	}
}

type fakeSnapshotSink struct {
	*bytes.Buffer
	id string
}

func (s *fakeSnapshotSink) ID() string    { return s.id }
func (s *fakeSnapshotSink) Cancel() error { return nil }
func (s *fakeSnapshotSink) Close() error  { return nil }

// TestSnapshotAndRestore_RoundTrip exercises Snapshot -> Persist -> Restore
// end to end against a real bbolt-backed StateMachine — this path has never
// been exercised anywhere in the test suite before. It caught two real bugs
// on first write: Restore wrote to a hardcoded "state.db" path instead of
// the file storage.New actually opened ("scheduler.db"), and it reassigned
// sm.db without ever rebuilding sm.storage — the interface every Apply
// actually goes through — so a real snapshot install would have silently
// left the state machine operating on a closed database. Both are fixed in
// Restore; this test would fail (or panic) against the old implementation.
func TestSnapshotAndRestore_RoundTrip(t *testing.T) {
	sm := newTestStateMachine(t)

	sm.Apply(logEntry(t, "create_tenant", command.CreateTenantCommand{ID: "t1", Name: "Acme", APIKeyHash: "test-hash"}, time.Now()))

	snap, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &fakeSnapshotSink{Buffer: &bytes.Buffer{}, id: "snap-1"}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()

	// Written after the snapshot was taken — must not survive Restore.
	sm.Apply(logEntry(t, "create_tenant", command.CreateTenantCommand{ID: "t2", Name: "Should not survive restore", APIKeyHash: "test-hash"}, time.Now()))

	if err := sm.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got := getTenant(t, sm, "t1"); got == nil {
		t.Error("tenant t1 (present at snapshot time) missing after restore — round-trip lost data")
	}
	if got := getTenant(t, sm, "t2"); got != nil {
		t.Error("tenant t2 (written after the snapshot) present after restore — Restore did not actually replace storage with the snapshot")
	}

	// Prove sm.storage itself was rebuilt around the new db handle, not just
	// sm.db: a write after Restore must actually persist.
	sm.Apply(logEntry(t, "create_tenant", command.CreateTenantCommand{ID: "t3", Name: "Post-restore write", APIKeyHash: "test-hash"}, time.Now()))
	if got := getTenant(t, sm, "t3"); got == nil {
		t.Error("write after Restore did not persist — sm.storage was not rebuilt around the new db handle")
	}
}
