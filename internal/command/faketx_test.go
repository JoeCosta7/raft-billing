package command

import (
	"encoding/json"
	"raft-biling/internal/model"
	"sort"
	"time"
)

type fakeTx struct {
	schedules  map[string]*model.Schedule
	executions map[string]*model.Execution
	attempts   map[string]*model.Attempt
	tenants    map[string]*model.Tenant
}

func newFakeTx() *fakeTx {
	return &fakeTx{
		schedules:  make(map[string]*model.Schedule),
		executions: make(map[string]*model.Execution),
		attempts:   make(map[string]*model.Attempt),
		tenants:    make(map[string]*model.Tenant),
	}
}

func (f *fakeTx) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	key := tenantID + ":" + id
	s, ok := f.schedules[key]
	if !ok {
		return nil, nil
	}
	return s, nil
}

func (f *fakeTx) PutSchedule(s *model.Schedule) error {
	key := s.TenantID + ":" + s.ID
	f.schedules[key] = s
	return nil
}

func (f *fakeTx) GetExecution(tenantID, id string) (*model.Execution, error) {
	key := tenantID + ":" + id
	s, ok := f.executions[key]
	if !ok {
		return nil, nil
	}
	return s, nil
}

func (f *fakeTx) PutExecution(ex *model.Execution) error {
	key := ex.TenantID + ":" + ex.ID
	f.executions[key] = ex
	return nil
}

func (f *fakeTx) GetAttempt(tenantID, id string) (*model.Attempt, error) {
	key := tenantID + ":" + id
	s, ok := f.attempts[key]
	if !ok {
		return nil, nil
	}
	return s, nil
}

func (f *fakeTx) PutAttempt(a *model.Attempt) error {
	key := a.TenantID + ":" + a.ID
	f.attempts[key] = a
	return nil
}

func (f *fakeTx) GetTenant(id string) (*model.Tenant, error) {
	t, ok := f.tenants[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (f *fakeTx) PutTenant(t *model.Tenant) error {
	f.tenants[t.ID] = t
	return nil
}

func (f *fakeTx) ListTenants() ([]*model.Tenant, error) {
	var tenants []*model.Tenant
	for _, t := range f.tenants {
		tenants = append(tenants, t)
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i].ID < tenants[j].ID })
	return tenants, nil
}

func (f *fakeTx) ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error {
	// collect matching executions into a sortable slice
	var matches []*model.Execution
	for _, ex := range f.executions {
		if ex.TenantID == tenantID && ex.ScheduleID == scheduleID {
			matches = append(matches, ex)
		}
	}
	// sort by ID for deterministic order
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	for _, ex := range matches {
		if err := fn(ex); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeTx) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error {
	var matches []*model.Execution
	for _, ex := range f.executions {
		if ex.TenantID == tenantID && ex.Status == status {
			matches = append(matches, ex)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	for _, ex := range matches {
		if err := fn(ex); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeTx) ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error {
	var matches []*model.Attempt
	for _, at := range f.attempts {
		if at.TenantID == tenantID && at.ExecutionID == executionID {
			matches = append(matches, at)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	for _, at := range matches {
		if err := fn(at); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeTx) ListSchedulesDue(tenantID string, now time.Time, fn func(*model.Schedule) error) error {
	var matches []*model.Schedule
	for _, s := range f.schedules {
		if s.TenantID != tenantID || s.Status != model.ScheduleStatusActive {
			continue
		}
		if s.NextRunAt == nil || s.NextRunAt.After(now) {
			continue
		}
		matches = append(matches, s)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].ID < matches[j].ID })
	for _, s := range matches {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
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

func newTestPausedSchedule(opts ...func(*model.Schedule)) *model.Schedule {
	seconds := 3600
	s := &model.Schedule{
		SchemaVersion: 1,
		ID:            testScheduleID,
		TenantID:      testTenantID,
		Status:        model.ScheduleStatusPaused,
		ScheduleType:  model.ScheduleTypeRecurring,
		CatchUpPolicy: model.CatchUpPolicyAll,
		Timezone:      "UTC",
		FirstRunAt:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Recurrence: &model.Recurrence{
			Cadence: model.CadenceInterval,
			Seconds: &seconds,
		},
		CreatedAt:   testTime,
		UpdatedAt:   testTime,
		MaxAttempts: 5,
		RetryBackoff: model.RetryBackoff{
			Initial:    30 * time.Second,
			Multiplier: 2.0,
			Max:        1 * time.Hour,
		},
		CallTimeout: durationPtr(5 * time.Second),
		NextRunAt:   nil,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func newTestResumeScheduleCommand(opts ...func(*ResumeScheduleCommand)) *ResumeScheduleCommand {
	cmd := &ResumeScheduleCommand{
		TenantID: testTenantID,
		ID:       testScheduleID,
	}
	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

func newTestExecution(opts ...func(*model.Execution)) *model.Execution {
	ex := &model.Execution{
		SchemaVersion:  1,
		ID:             testExecutionID,
		TenantID:       testTenantID,
		ScheduleID:     testScheduleID,
		ScheduledFor:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: testScheduleID + ":" + testExecutionID,
		Status:         model.ExecutionStatusSucceeded,
		AttemptCount:   1,
	}
	for _, opt := range opts {
		opt(ex)
	}
	return ex
}

func seedExecution(tx *fakeTx, ex *model.Execution) error {
	existing, err := tx.GetExecution(ex.TenantID, ex.ID)
	if err != nil {
		return err
	}
	if existing != nil {
		return nil
	}
	return tx.PutExecution(ex)
}
