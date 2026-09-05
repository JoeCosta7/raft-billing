package storage

import (
	"encoding/json"
	"raft-biling/internal/model"
	"reflect"
	"slices"
	"testing"
	"time"
)

var testTime = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

func durationPtr(d time.Duration) *time.Duration { return &d }

const (
	testTenantID    = "default"
	testScheduleID  = "sched_test_01"
	testExecutionID = "exec_test_01"
	testAttemptID   = "att_test_01"
)

const (
	tenantA = "tenant_a"
	tenantB = "tenant_b"
	sched1a = "sched_1a"
	sched1b = "sched_1b"
	schedB1 = "sched_b1"
	exec1a  = "exec_1a"
	exec1b  = "exec_1b"
	exec1c  = "exec_1c"
	atte1a  = "atte_1a"
	atte1b  = "atte_1b"
	atte1c  = "atte_1c"
)

func newTestStorage(t *testing.T) *BoltStorage {
	t.Helper()
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func newTestSchedule(overrides ...func(*model.Schedule)) *model.Schedule {
	dayOfMonth := 15
	sch := &model.Schedule{
		SchemaVersion: 1,
		ID:            testScheduleID,
		TenantID:      testTenantID,
		CreatedAt:     testTime,
		UpdatedAt:     testTime,
		CreatedBy:     "test",
		CallbackURL:   "https://example.com/webhook",
		Payload:       json.RawMessage(`{"amount":100}`),
		Headers:       map[string]string{"X-Test": "hello"},
		ScheduleType:  model.ScheduleTypeRecurring,
		FirstRunAt:    testTime,
		Recurrence: &model.Recurrence{
			Cadence:    model.CadenceMonthly,
			DayOfMonth: &dayOfMonth,
		},
		Timezone:    "UTC",
		MaxAttempts: 5,
		RetryBackoff: model.RetryBackoff{
			Initial:    30 * time.Second,
			Multiplier: 2.0,
			Max:        1 * time.Hour,
		},
		CallTimeout:   durationPtr(5 * time.Second),
		CatchUpPolicy: model.CatchUpPolicyAll,
		Status:        model.ScheduleStatusActive,
		NextRunAt:     &testTime,
	}
	for _, o := range overrides {
		o(sch)
	}
	return sch
}

func newTestExecution(overrides ...func(*model.Execution)) *model.Execution {
	ex := &model.Execution{
		SchemaVersion:  1,
		ID:             testExecutionID,
		TenantID:       testTenantID,
		ScheduleID:     testScheduleID,
		ScheduledFor:   testTime,
		IdempotencyKey: "first",
		OwnerNodeID:    "node1",
		ClaimedAt:      &testTime,
		Status:         model.ExecutionStatusInFlight,
		StartedAt:      &testTime,
		CompletedAt:    &testTime,
		AttemptCount:   1,
		LastAttemptID:  "last",
		FinalOutcome:   "completed successfully",
	}
	for _, o := range overrides {
		o(ex)
	}
	return ex
}

func newTestAttempt(overrides ...func(*model.Attempt)) *model.Attempt {
	at := &model.Attempt{
		SchemaVersion:       1,
		ID:                  testAttemptID,
		TenantID:            testTenantID,
		ExecutionID:         testExecutionID,
		NodeID:              "node1",
		AttemptNumber:       1,
		StartedAt:           &testTime,
		CompletedAt:         &testTime,
		RequestURL:          "https://example.com/webhook",
		RequestHeaders:      map[string]string{"X-Test": "hello"},
		RequestBodyHash:     "sha256:abc123",
		ResponseStatus:      200,
		ResponseBodyExcerpt: "ok",
		ResponseHeaders:     map[string]string{"Content-Type": "application/json"},
		Outcome:             model.OutcomeSuccess,
	}
	for _, o := range overrides {
		o(at)
	}
	return at
}
func TestStorage_ScheduleRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	schedule := newTestSchedule()
	if err := s.Update(func(tx Tx) error { return tx.PutSchedule(schedule) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Schedule
	err := s.View(func(tx Tx) error {
		result, err := tx.GetSchedule(schedule.TenantID, schedule.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, schedule) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, schedule)
	}
}

func TestStorage_ExecutionRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	execution := newTestExecution()
	if err := s.Update(func(tx Tx) error { return tx.PutExecution(execution) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Execution
	err := s.View(func(tx Tx) error {
		result, err := tx.GetExecution(execution.TenantID, execution.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, execution) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, execution)
	}

}

func TestStorage_AttemptRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	attempt := newTestAttempt()
	if err := s.Update(func(tx Tx) error { return tx.PutAttempt(attempt) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Attempt
	err := s.View(func(tx Tx) error {
		result, err := tx.GetAttempt(attempt.TenantID, attempt.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, attempt) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, attempt)
	}

}

func TestStorage_GetMissingReturnsNil(t *testing.T) {
	s := newTestStorage(t)
	err := s.View(func(tx Tx) error {
		gots, err := tx.GetSchedule(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gots != nil {
			t.Errorf("GetSchedule: got %v, want nil", gots)
		}
		gotex, err := tx.GetExecution(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gotex != nil {
			t.Errorf("GetExecution: got %v, want nil", gotex)
		}
		gotat, err := tx.GetAttempt(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gotat != nil {
			t.Errorf("GetAttempt: got %v, want nil", gotat)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestStorage_PutExecution_StatusTransitionCleansStaleIndex(t *testing.T) {
	s := newTestStorage(t)
	execution := newTestExecution(func(e *model.Execution) { e.Status = model.ExecutionStatusPending; e.ID = "exec_1" })
	if err := s.Update(func(tx Tx) error { return tx.PutExecution(execution) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	execution.Status = model.ExecutionStatusInFlight
	if err := s.Update(func(tx Tx) error { return tx.PutExecution(execution) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	err := s.View(func(tx Tx) error {
		pendingCount := 0
		if err := tx.ListExecutionsByStatus(testTenantID, model.ExecutionStatusPending, func(e *model.Execution) error {
			pendingCount++
			return nil
		}); err != nil {
			return err
		}
		if pendingCount != 0 {
			t.Errorf("ListExecutionsByStatus Pending Count: got %d, want 0", pendingCount)
		}
		inFlightCount := 0
		if err := tx.ListExecutionsByStatus(testTenantID, model.ExecutionStatusInFlight, func(e *model.Execution) error {
			inFlightCount++
			if e.Status != model.ExecutionStatusInFlight {
				t.Errorf("execution %s: status in row is %s, want %s", e.ID, e.Status, model.ExecutionStatusInFlight)
			}
			return nil
		}); err != nil {
			return err
		}
		if inFlightCount != 1 {
			t.Errorf("ListExecutionsByStatus In Flight Count: got %d, want 1", inFlightCount)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestStorage_ListExecutionsBySchedule_HappyPath(t *testing.T) {
	s := newTestStorage(t)
	schedule := newTestSchedule(func(e *model.Schedule) { e.ID = sched1a })
	expected := []string{"exec_1a", "exec_1b", "exec_1c"}
	ids := []string{}
	executionA := newTestExecution(func(e *model.Execution) { e.ID = exec1a; e.ScheduleID = schedule.ID })
	executionB := newTestExecution(func(e *model.Execution) { e.ID = exec1b; e.ScheduleID = schedule.ID })
	executionC := newTestExecution(func(e *model.Execution) { e.ID = exec1c; e.ScheduleID = schedule.ID })
	if err := s.Update(func(tx Tx) error {
		if err := tx.PutExecution(executionA); err != nil {
			return err
		}
		if err := tx.PutExecution(executionB); err != nil {
			return err
		}
		return tx.PutExecution(executionC)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := s.View(func(tx Tx) error {
		return tx.ListExecutionsBySchedule(schedule.TenantID, schedule.ID, func(e *model.Execution) error {
			ids = append(ids, e.ID)
			return nil
		})
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	if !slices.Equal(ids, expected) {
		t.Fatalf("IDS mismatch:\ngot:  %+v\nwant: %+v", ids, expected)
	}
}

func TestStorage_ListExecutionsByStatus_HappyPath(t *testing.T) {
	s := newTestStorage(t)
	expected := []string{"exec_1a", "exec_1b"}
	ids := []string{}
	executionA := newTestExecution(func(e *model.Execution) { e.ID = exec1a; e.Status = model.ExecutionStatusPending })
	executionB := newTestExecution(func(e *model.Execution) { e.ID = exec1b; e.Status = model.ExecutionStatusPending })
	executionC := newTestExecution(func(e *model.Execution) {
		e.ID = exec1c
		e.Status = model.ExecutionStatusSucceeded
	})
	if err := s.Update(func(tx Tx) error {
		if err := tx.PutExecution(executionA); err != nil {
			return err
		}
		if err := tx.PutExecution(executionB); err != nil {
			return err
		}
		return tx.PutExecution(executionC)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := s.View(func(tx Tx) error {
		return tx.ListExecutionsByStatus(testTenantID, model.ExecutionStatusPending, func(e *model.Execution) error {
			ids = append(ids, e.ID)
			return nil
		})
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	if !slices.Equal(ids, expected) {
		t.Fatalf("ids mismatch:\ngot:  %+v\nwant: %+v", ids, expected)
	}

}

func TestStorage_ListAttemptsByExecution_HappyPath(t *testing.T) {
	s := newTestStorage(t)
	execution := newTestExecution(func(e *model.Execution) { e.ID = exec1a })
	expected := []string{"atte_1a", "atte_1b", "atte_1c"}
	ids := []string{}
	attemptA := newTestAttempt(func(a *model.Attempt) { a.ID = atte1a; a.ExecutionID = execution.ID })
	attemptB := newTestAttempt(func(a *model.Attempt) { a.ID = atte1b; a.ExecutionID = execution.ID })
	attemptC := newTestAttempt(func(a *model.Attempt) { a.ID = atte1c; a.ExecutionID = execution.ID })
	if err := s.Update(func(tx Tx) error {
		if err := tx.PutAttempt(attemptA); err != nil {
			return err
		}
		if err := tx.PutAttempt(attemptB); err != nil {
			return err
		}
		return tx.PutAttempt(attemptC)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := s.View(func(tx Tx) error {
		return tx.ListAttemptsByExecution(testTenantID, execution.ID, func(a *model.Attempt) error {
			ids = append(ids, a.ID)
			return nil
		})
	}); err != nil {
		t.Fatalf("View: %v", err)
	}

	if !slices.Equal(ids, expected) {
		t.Fatalf("ids mismatch:\ngot:  %+v\nwant: %+v", ids, expected)
	}
}
