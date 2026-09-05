package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"raft-biling/internal/command"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
)

type fakeReader struct {
	tenants        []*model.Tenant
	execsByTenant  map[string][]*model.Execution
	attemptsByExec map[string][]*model.Attempt

	listTenantsErr    error
	listExecutionsErr map[string]error
	listAttemptsErr   map[string]error
}

func (f *fakeReader) ListTenants() ([]*model.Tenant, error) {
	if f.listTenantsErr != nil {
		return nil, f.listTenantsErr
	}
	return f.tenants, nil
}

func (f *fakeReader) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error) {
	if err := f.listExecutionsErr[tenantID]; err != nil {
		return nil, err
	}
	return f.execsByTenant[tenantID], nil
}

func (f *fakeReader) ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error) {
	if err := f.listAttemptsErr[executionID]; err != nil {
		return nil, err
	}
	return f.attemptsByExec[executionID], nil
}

func (f *fakeReader) ListSchedulesDue(tenantID string) ([]*model.Schedule, error) {
	return nil, nil
}

func (f *fakeReader) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	return nil, nil
}

func ptrTime(t time.Time) *time.Time { return &t }

type boltReader struct {
	store storage.Storage
}

func (r *boltReader) ListTenants() ([]*model.Tenant, error) {
	var tenants []*model.Tenant
	err := r.store.View(func(tx storage.Tx) error {
		ts, err := tx.ListTenants()
		if err != nil {
			return err
		}
		tenants = ts
		return nil
	})
	return tenants, err
}

func (r *boltReader) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	var schedule *model.Schedule
	err := r.store.View(func(tx storage.Tx) error {
		s, err := tx.GetSchedule(tenantID, id)
		if err != nil {
			return err
		}
		schedule = s
		return nil
	})
	return schedule, err
}

func (r *boltReader) ListSchedulesDue(tenantID string) ([]*model.Schedule, error) {
	return nil, nil
}

func (r *boltReader) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error) {
	var execs []*model.Execution
	err := r.store.View(func(tx storage.Tx) error {
		return tx.ListExecutionsByStatus(tenantID, status, func(ex *model.Execution) error {
			execs = append(execs, ex)
			return nil
		})
	})
	return execs, err
}

func (r *boltReader) ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error) {
	var attempts []*model.Attempt
	err := r.store.View(func(tx storage.Tx) error {
		return tx.ListAttemptsByExecution(tenantID, executionID, func(a *model.Attempt) error {
			attempts = append(attempts, a)
			return nil
		})
	})
	return attempts, err
}

func (r *boltReader) GetExecution(tenantID, id string) (*model.Execution, error) {
	var exec *model.Execution
	err := r.store.View(func(tx storage.Tx) error {
		ex, err := tx.GetExecution(tenantID, id)
		if err != nil {
			return err
		}
		exec = ex
		return nil
	})
	return exec, err
}

type proposal struct {
	cmdType string
	cmd     any
	timeout time.Duration
}

type applyingProposer struct {
	store    storage.Storage
	nodeID   string
	forceErr error // when set, Propose fails before ever touching store or calls — for exercising the transport-error path
	mu       sync.Mutex
	calls    []proposal
}

func newApplyingProposer(t *testing.T, nodeID string) *applyingProposer {
	t.Helper()
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return &applyingProposer{store: store, nodeID: nodeID}
}

func (p *applyingProposer) ID() string                { return p.nodeID }
func (p *applyingProposer) TransferLeadership() error { return nil }

func (p *applyingProposer) Propose(cmdType string, cmd any, timeout time.Duration) (any, error) {
	if p.forceErr != nil {
		return nil, p.forceErr
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", cmdType, err)
	}

	p.mu.Lock()
	p.calls = append(p.calls, proposal{cmdType: cmdType, cmd: cmd, timeout: timeout})
	p.mu.Unlock()

	var result any
	err = p.store.Update(func(tx storage.Tx) error {
		r, e := command.Dispatch(tx, cmdType, payload, time.Now())
		result = r
		return e
	})
	var cmdErr *command.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr, nil // CommandError as result, not error — mirrors real Apply
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

func TestWorker_KindRetryWritesLastRetryAt(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		priorOwner = "prior-leader"
		selfNodeID = "test-node"
	)

	requestCount := 0
	var mu sync.Mutex
	firstRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		n := requestCount
		mu.Unlock()
		if n == 1 {
			close(firstRequest)
		}
		w.WriteHeader(500)
	}))
	defer server.Close()

	// storage
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	defer store.Close()

	// pre-populate
	prePopulatedLastRetry := time.Now().Add(-5 * time.Minute)
	err = store.Update(func(tx storage.Tx) error {
		if err := tx.PutTenant(&model.Tenant{ID: tenantID}); err != nil {
			return err
		}
		if err := tx.PutSchedule(&model.Schedule{
			TenantID:     tenantID,
			ID:           scheduleID,
			Status:       model.ScheduleStatusActive,
			CallbackURL:  server.URL,
			Payload:      []byte("{}"),
			MaxAttempts:  3,
			RetryBackoff: model.RetryBackoff{Initial: 100 * time.Millisecond, Multiplier: 1},
			// NextRunAt: nil — keeps fresh-fire scan uninteresting
		}); err != nil {
			return err
		}
		return tx.PutExecution(&model.Execution{
			TenantID:     tenantID,
			ID:           execID,
			ScheduleID:   scheduleID,
			Status:       model.ExecutionStatusInFlight,
			OwnerNodeID:  priorOwner,
			AttemptCount: 1,
			LastRetryAt:  &prePopulatedLastRetry,
			ClaimedAt:    ptrTime(time.Now().Add(-1 * time.Minute)),
		})
	})
	if err != nil {
		t.Fatalf("pre-populate: %v", err)
	}

	// scaffold
	reader := &boltReader{store: store}
	proposer := &applyingProposer{store: store, nodeID: selfNodeID}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))
	w.tickInterval = 20 * time.Millisecond
	w.poolSize = 1

	// run
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// wait for the first HTTP request (business attempt #2, since AttemptCount starts at 1)
	select {
	case <-firstRequest:
	case <-time.After(5 * time.Second):
		t.Fatal("first HTTP request did not arrive within 5s")
	}

	// poll for that request's RecordAttempt to land
	deadline := time.Now().Add(1 * time.Second)
	var exec *model.Execution
	for time.Now().Before(deadline) {
		exec, err = reader.GetExecution(tenantID, execID)
		if err == nil && exec != nil && exec.AttemptCount == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if exec == nil || exec.AttemptCount != 2 {
		t.Fatalf("execution never reached AttemptCount=2; got %+v", exec)
	}

	// LOAD-BEARING ASSERTION
	if exec.LastRetryAt == nil {
		t.Fatal("LastRetryAt is nil after attempt 2 — KindRetry regression: RecordAttempt did not write LastRetryAt")
	}
	if !exec.LastRetryAt.After(prePopulatedLastRetry) {
		t.Errorf("LastRetryAt not advanced: got %v, want > %v", exec.LastRetryAt, prePopulatedLastRetry)
	}

	// teardown
	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Run: unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of ctx cancel")
	}
}

func TestClassifyOne_EmptyAttempts(t *testing.T) {
	bucket, err := classifyOne(model.Execution{}, nil, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketNoAttempts {
		t.Errorf("bucket: got %q, want %q", bucket, BucketNoAttempts)
	}
}

func TestClassifyOne_LastAttemptSuccess(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeSuccess},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketSuccess {
		t.Errorf("bucket: got %q, want %q", bucket, BucketSuccess)
	}
}

func TestClassifyOne_LastAttemptTerminalFailure(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeTerminalFailure},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketTerminalFailure {
		t.Errorf("bucket: got %q, want %q", bucket, BucketTerminalFailure)
	}
}

func TestClassifyOne_RetryElapsed(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(1 * time.Hour) // well past RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q", bucket, BucketRetryElapsed)
	}
}

func TestClassifyOne_RetryPending(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(-1 * time.Hour) // well before RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryPending {
		t.Errorf("bucket: got %q, want %q", bucket, BucketRetryPending)
	}
}

func TestClassifyOne_RetryBoundary(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q (now == RetryAt must count as elapsed)", bucket, BucketRetryElapsed)
	}
}

func TestClassifyOne_UnknownOutcome(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.AttemptOutcome("bogus_outcome")},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err == nil {
		t.Fatal("classifyOne: expected error for unknown outcome, got nil")
	}
	if bucket != "" {
		t.Errorf("bucket: got %q, want empty on error", bucket)
	}
	if !strings.Contains(err.Error(), "bogus_outcome") {
		t.Errorf("error message: got %q, want it to mention the outcome value", err.Error())
	}
}

func TestClassifyOne_MultipleAttempts_LastMatters(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(1 * time.Hour) // well past RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeSuccess},
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q (should classify off the last attempt, not the first)", bucket, BucketRetryElapsed)
	}
}

func TestRunRecover_Buckets(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		execID     = "exec-1"
		nodeID     = "node-self"
		otherOwner = "node-other"
	)
	now := time.Now()

	tests := []struct {
		name        string
		ownerNodeID string
		attempts    []*model.Attempt
		wantCmdType string
		wantCmd     any
	}{
		{
			name:        "no attempts",
			ownerNodeID: otherOwner,
			attempts:    nil,
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "retry elapsed",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeRetry, RetryAt: now.Add(-1 * time.Hour)},
			},
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "retry pending",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeRetry, RetryAt: now.Add(1 * time.Hour)},
			},
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "last attempt success",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeSuccess},
			},
			wantCmdType: "complete_execution",
			wantCmd: command.CompleteExecutionCommand{
				TenantID:     tenantID,
				ExecutionID:  execID,
				FinalStatus:  model.ExecutionStatusSucceeded,
				FinalOutcome: string(model.OutcomeSuccess),
			},
		},
		{
			name:        "last attempt terminal failure",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeTerminalFailure},
			},
			wantCmdType: "complete_execution",
			wantCmd: command.CompleteExecutionCommand{
				TenantID:     tenantID,
				ExecutionID:  execID,
				FinalStatus:  model.ExecutionStatusFailedTerminal,
				FinalOutcome: string(model.OutcomeTerminalFailure),
			},
		},
		{
			name:        "already own it",
			ownerNodeID: nodeID,
			attempts:    nil,
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: nodeID,
				NewOwner:      nodeID,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeReader{
				tenants: []*model.Tenant{{ID: tenantID}},
				execsByTenant: map[string][]*model.Execution{
					tenantID: {{ID: execID, TenantID: tenantID, OwnerNodeID: tc.ownerNodeID, Status: model.ExecutionStatusInFlight}},
				},
				attemptsByExec: map[string][]*model.Attempt{
					execID: tc.attempts,
				},
			}
			proposer := newApplyingProposer(t, nodeID)
			w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

			if err := w.runRecover(context.Background()); err != nil {
				t.Fatalf("runRecover: unexpected error: %v", err)
			}

			if len(proposer.calls) != 1 {
				t.Fatalf("calls: got %d, want 1: %+v", len(proposer.calls), proposer.calls)
			}
			got := proposer.calls[0]
			if got.cmdType != tc.wantCmdType {
				t.Errorf("cmdType: got %q, want %q", got.cmdType, tc.wantCmdType)
			}
			if !reflect.DeepEqual(got.cmd, tc.wantCmd) {
				t.Errorf("cmd: got %+v, want %+v", got.cmd, tc.wantCmd)
			}
		})
	}
}

func TestRunRecover_ListTenantsError(t *testing.T) {
	reader := &fakeReader{listTenantsErr: errors.New("boom")}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list tenants") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "list tenants")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0 — nothing should have been proposed", len(proposer.calls))
	}
}

func TestRunRecover_ListExecutionsError(t *testing.T) {
	const tenantID = "tenant-1"
	reader := &fakeReader{
		tenants:           []*model.Tenant{{ID: tenantID}},
		listExecutionsErr: map[string]error{tenantID: errors.New("boom")},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list in-flight") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "list in-flight")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ListAttemptsError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		listAttemptsErr: map[string]error{execID: errors.New("boom")},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "read attempts") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "read attempts")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ProposeError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, OwnerNodeID: "node-other", Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{execID: nil}, // BucketNoAttempts -> adopt path
	}
	proposer := newApplyingProposer(t, "node-self")
	proposer.forceErr = errors.New("boom")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "propose adopt") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "propose adopt")
	}
}

func TestRunRecover_ContextCancelled(t *testing.T) {
	const tenantID = "tenant-1"
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.runRecover(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runRecover: got %v, want context.Canceled", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ClassificationError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execID: {{Outcome: model.AttemptOutcome("bogus_outcome")}},
		},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "classify") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "classify")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_PanicRecovery(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execID: {nil}, // classifyOne dereferences the last element -> nil pointer panic
		},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error from recovered panic, got nil")
	}
	if !strings.Contains(err.Error(), "recover: panic") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "recover: panic")
	}
}

func TestRunRecover_MultiTenant_ErrorOnSecondTenant(t *testing.T) {
	const (
		tenantA = "tenant-a"
		tenantB = "tenant-b"
		execA   = "exec-a"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantA}, {ID: tenantB}},
		execsByTenant: map[string][]*model.Execution{
			tenantA: {{ID: execA, TenantID: tenantA, OwnerNodeID: "node-other", Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execA: nil, // BucketNoAttempts -> adopt
		},
		listExecutionsErr: map[string]error{
			tenantB: errors.New("boom"),
		},
	}
	proposer := newApplyingProposer(t, "node-self")
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list in-flight") || !strings.Contains(err.Error(), tenantB) {
		t.Errorf("error: got %q, want it to mention tenant %q and the list in-flight failure", err.Error(), tenantB)
	}
	if len(proposer.calls) != 1 {
		t.Fatalf("calls: got %d, want 1 (tenant A should have been fully processed before tenant B failed)", len(proposer.calls))
	}
	if proposer.calls[0].cmdType != "adopt_execution" {
		t.Errorf("cmdType: got %q, want %q", proposer.calls[0].cmdType, "adopt_execution")
	}
}

// seedActiveSchedule puts a tenant and an active schedule (with an unreachable
// callback URL) into the given proposer's store, for tests that dispatch a
// pre-canceled context and never expect a real network call to happen.
func seedActiveSchedule(t *testing.T, p *applyingProposer, tenantID, scheduleID string) {
	t.Helper()
	err := p.store.Update(func(tx storage.Tx) error {
		if err := tx.PutTenant(&model.Tenant{ID: tenantID}); err != nil {
			return err
		}
		return tx.PutSchedule(&model.Schedule{
			TenantID:    tenantID,
			ID:          scheduleID,
			Status:      model.ScheduleStatusActive,
			CallbackURL: "http://127.0.0.1:1/unreachable",
			Payload:     []byte("{}"),
			MaxAttempts: 3,
		})
	})
	if err != nil {
		t.Fatalf("seedActiveSchedule: %v", err)
	}
}

func TestFreshFireTask_Run_DoErrCanceled_SkipsProposal(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		nodeID     = "test-node"
	)

	proposer := newApplyingProposer(t, nodeID)
	seedActiveSchedule(t, proposer, tenantID, scheduleID)

	task := &freshFireTask{
		exec: model.Execution{
			TenantID:   tenantID,
			ID:         execID,
			ScheduleID: scheduleID,
		},
		nodeID:             nodeID,
		reader:             &boltReader{store: proposer.store},
		proposer:           proposer,
		httpClient:         http.DefaultClient,
		logger:             slog.New(slog.DiscardHandler),
		defaultCallTimeout: model.DefaultCallTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := task.run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run: got %v, want context.Canceled", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0 — canceled dispatch must not propose anything", len(proposer.calls))
	}
}

func TestInFlightTask_RunRetry_DoErrCanceled_SkipsProposal(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		nodeID     = "test-node"
	)

	proposer := newApplyingProposer(t, nodeID)
	seedActiveSchedule(t, proposer, tenantID, scheduleID)

	task := &inFlightTask{
		exec: model.Execution{
			TenantID:     tenantID,
			ID:           execID,
			ScheduleID:   scheduleID,
			AttemptCount: 1,
		},
		kind:               KindRetry,
		reader:             &boltReader{store: proposer.store},
		proposer:           proposer,
		httpClient:         http.DefaultClient,
		logger:             slog.New(slog.DiscardHandler),
		defaultCallTimeout: model.DefaultCallTimeout,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := task.run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run: got %v, want context.Canceled", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0 — canceled dispatch must not propose anything", len(proposer.calls))
	}
}

func durationPtr(d time.Duration) *time.Duration { return &d }

func TestFreshFireTask_Run_CallTimeoutFires_RecordsRetry(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		nodeID     = "test-node"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	proposer := newApplyingProposer(t, nodeID)
	err := proposer.store.Update(func(tx storage.Tx) error {
		if err := tx.PutTenant(&model.Tenant{ID: tenantID}); err != nil {
			return err
		}
		if err := tx.PutSchedule(&model.Schedule{
			TenantID:    tenantID,
			ID:          scheduleID,
			Status:      model.ScheduleStatusActive,
			CallbackURL: server.URL,
			Payload:     []byte("{}"),
			MaxAttempts: 3,
			CallTimeout: durationPtr(10 * time.Millisecond),
		}); err != nil {
			return err
		}
		return tx.PutExecution(&model.Execution{
			TenantID:    tenantID,
			ID:          execID,
			ScheduleID:  scheduleID,
			Status:      model.ExecutionStatusInFlight,
			OwnerNodeID: nodeID,
		})
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	task := &freshFireTask{
		exec: model.Execution{
			TenantID:   tenantID,
			ID:         execID,
			ScheduleID: scheduleID,
		},
		nodeID:             nodeID,
		reader:             &boltReader{store: proposer.store},
		proposer:           proposer,
		httpClient:         &http.Client{},
		logger:             slog.New(slog.DiscardHandler),
		defaultCallTimeout: model.DefaultCallTimeout,
	}

	if err := task.run(context.Background()); err != nil {
		t.Fatalf("run: unexpected error: %v", err)
	}

	if len(proposer.calls) != 1 {
		t.Fatalf("calls: got %d, want 1 (record_attempt only, no complete): %+v", len(proposer.calls), proposer.calls)
	}
	recordCmd, ok := proposer.calls[0].cmd.(command.RecordAttemptCommand)
	if !ok {
		t.Fatalf("calls[0].cmd: got %T, want command.RecordAttemptCommand", proposer.calls[0].cmd)
	}
	if recordCmd.Outcome != model.OutcomeRetry {
		t.Errorf("Outcome: got %q, want %q — CallTimeout firing must be a real, retryable attempt outcome, not silently dropped", recordCmd.Outcome, model.OutcomeRetry)
	}
}

func TestFreshFireTask_Run_NilCallTimeout_UsesDefault(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		nodeID     = "test-node"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	proposer := newApplyingProposer(t, nodeID)
	err := proposer.store.Update(func(tx storage.Tx) error {
		if err := tx.PutTenant(&model.Tenant{ID: tenantID}); err != nil {
			return err
		}
		if err := tx.PutSchedule(&model.Schedule{
			TenantID:    tenantID,
			ID:          scheduleID,
			Status:      model.ScheduleStatusActive,
			CallbackURL: server.URL,
			Payload:     []byte("{}"),
			MaxAttempts: 3,
			// CallTimeout intentionally left nil — tracks the current system
			// default (also covers a schedule persisted before this field existed,
			// which decodes to nil the same way).
		}); err != nil {
			return err
		}
		return tx.PutExecution(&model.Execution{
			TenantID:    tenantID,
			ID:          execID,
			ScheduleID:  scheduleID,
			Status:      model.ExecutionStatusInFlight,
			OwnerNodeID: nodeID,
		})
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	task := &freshFireTask{
		exec: model.Execution{
			TenantID:   tenantID,
			ID:         execID,
			ScheduleID: scheduleID,
		},
		nodeID:             nodeID,
		reader:             &boltReader{store: proposer.store},
		proposer:           proposer,
		httpClient:         &http.Client{},
		logger:             slog.New(slog.DiscardHandler),
		defaultCallTimeout: model.DefaultCallTimeout,
	}

	if err := task.run(context.Background()); err != nil {
		t.Fatalf("run: unexpected error: %v", err)
	}

	if len(proposer.calls) != 2 {
		t.Fatalf("calls: got %d, want 2 (record_attempt + complete_execution): %+v", len(proposer.calls), proposer.calls)
	}
	recordCmd, ok := proposer.calls[0].cmd.(command.RecordAttemptCommand)
	if !ok {
		t.Fatalf("calls[0].cmd: got %T, want command.RecordAttemptCommand", proposer.calls[0].cmd)
	}
	if recordCmd.Outcome != model.OutcomeSuccess {
		t.Errorf("Outcome: got %q, want %q — a nil CallTimeout must resolve to the default, not fail instantly", recordCmd.Outcome, model.OutcomeSuccess)
	}
}

func TestFreshFireTask_Run_SchedulerContentTypeWinsOverOperatorHeader(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		scheduleID = "sched-1"
		execID     = "exec-1"
		nodeID     = "test-node"
	)

	var gotContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	proposer := newApplyingProposer(t, nodeID)
	err := proposer.store.Update(func(tx storage.Tx) error {
		if err := tx.PutTenant(&model.Tenant{ID: tenantID}); err != nil {
			return err
		}

		if err := tx.PutSchedule(&model.Schedule{
			TenantID:    tenantID,
			ID:          scheduleID,
			Status:      model.ScheduleStatusActive,
			CallbackURL: server.URL,
			Payload:     []byte("{}"),
			MaxAttempts: 3,
			Headers:     map[string]string{"Content-Type": "text/plain"},
		}); err != nil {
			return err
		}
		return tx.PutExecution(&model.Execution{
			TenantID:    tenantID,
			ID:          execID,
			ScheduleID:  scheduleID,
			Status:      model.ExecutionStatusInFlight,
			OwnerNodeID: nodeID,
		})
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	task := &freshFireTask{
		exec: model.Execution{
			TenantID:   tenantID,
			ID:         execID,
			ScheduleID: scheduleID,
		},
		nodeID:             nodeID,
		reader:             &boltReader{store: proposer.store},
		proposer:           proposer,
		httpClient:         &http.Client{},
		logger:             slog.New(slog.DiscardHandler),
		defaultCallTimeout: model.DefaultCallTimeout,
	}

	if err := task.run(context.Background()); err != nil {
		t.Fatalf("run: unexpected error: %v", err)
	}

	if gotContentType != "application/json" {
		t.Errorf("Content-Type on the wire: got %q, want %q — scheduler value must win over the operator-supplied one", gotContentType, "application/json")
	}
}
