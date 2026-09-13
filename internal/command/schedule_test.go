package command

import (
	"encoding/json"
	"errors"
	"raft-biling/internal/model"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testTenantID    = "default"
	testScheduleID  = "sched_test_01"
	testExecutionID = "exec_test_01"
	testAttemptID   = "att_test_01"
)

var testTime = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

func strPtr(s string) *string                    { return &s }
func intPtr(i int) *int                          { return &i }
func durationPtr(d time.Duration) *time.Duration { return &d }

func newTestCreateScheduleCommand(overrides ...func(c *CreateScheduleCommand)) *CreateScheduleCommand {
	dayOfMonth := 15
	sch := &CreateScheduleCommand{
		ID:           testScheduleID,
		TenantID:     testTenantID,
		CreatedBy:    "test",
		CallbackURL:  "https://example.com/webhook",
		Payload:      json.RawMessage(`{"amount":100}`),
		Headers:      map[string]string{"X-Test": "hello"},
		ScheduleType: model.ScheduleTypeRecurring,
		FirstRunAt:   testTime,
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
	}
	for _, o := range overrides {
		o(sch)
	}
	return sch
}

func newTestUpdateScheduleCommand(overrides ...func(*UpdateScheduleCommand)) *UpdateScheduleCommand {
	cmd := &UpdateScheduleCommand{
		TenantID:     testTenantID,
		ID:           testScheduleID,
		CallbackURL:  nil,
		Headers:      nil,
		Payload:      nil,
		RetryBackoff: nil,
		MaxAttempts:  nil,
		CallTimeout:  nil,
	}
	for _, o := range overrides {
		o(cmd)
	}
	return cmd
}
func newTestPauseScheduleCommand(overrides ...func(*PauseScheduleCommand)) *PauseScheduleCommand {
	cmd := &PauseScheduleCommand{
		TenantID: testTenantID,
		ID:       testScheduleID,
	}
	for _, o := range overrides {
		o(cmd)
	}
	return cmd
}

func newTestCancelScheduleCommand(overrides ...func(*CancelScheduleCommand)) *CancelScheduleCommand {
	cmd := &CancelScheduleCommand{
		TenantID: testTenantID,
		ID:       testScheduleID,
	}
	for _, o := range overrides {
		o(cmd)
	}
	return cmd
}

func seedSchedule(f *fakeTx, s *model.Schedule) {
	f.schedules[s.TenantID+":"+s.ID] = s
}

func TestApplyCreateSchedule_OnceHappyPath(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ScheduleType = model.ScheduleTypeOnce })
	proposedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyCreateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyCreateSchedule: returned nil schedule with nil error")
	}
	if schedule.SchemaVersion != model.CurrentScheduleSchemaVersion {
		t.Errorf("SchemaVersion: got %d, want %d", schedule.SchemaVersion, model.CurrentScheduleSchemaVersion)
	}
	if !schedule.CreatedAt.Equal(proposedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, proposedAt)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if schedule.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", schedule.Status, model.ScheduleStatusActive)
	}
	if schedule.NextRunAt == nil {
		t.Errorf("NextRunAt: got nil, want non-nil")
	} else if !schedule.NextRunAt.Equal(cmd.FirstRunAt) {
		t.Errorf("NextRunAt: got %v, want %v", *schedule.NextRunAt, cmd.FirstRunAt)
	}
	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.ID != cmd.ID {
		t.Errorf("stored.ID: got %q, want %q", stored.ID, cmd.ID)
	}

}

func TestApplyCreateSchedule_RecurringHappyPath(t *testing.T) {
	tx := newFakeTx()
	seconds := 3600
	rec := &model.Recurrence{
		Cadence: model.CadenceInterval,
		Seconds: &seconds,
	}
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.ScheduleType = model.ScheduleTypeRecurring
		c.Recurrence = rec
		c.FirstRunAt = testTime
		c.Timezone = "UTC"
	})
	schedule, err := ApplyCreateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyCreateSchedule: returned nil schedule with nil error")
	}
	if schedule.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", schedule.Status, model.ScheduleStatusActive)
	}
	if schedule.NextRunAt == nil {
		t.Fatalf("NextRunAt is nil")
	}
	if !schedule.NextRunAt.Equal(cmd.FirstRunAt) {
		t.Error("NextRunAt not set correctly")
	}
}

func TestApplyCreateSchedule_RejectsReservedHeaderContentType(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.Headers = map[string]string{"Content-Type": "text/plain"}
	})
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "headers" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "headers")
	}
}

func TestApplyCreateSchedule_RejectsReservedHeaderCaseVariant(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.Headers = map[string]string{"content-type": "text/plain"}
	})
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if !strings.Contains(cmdErr.Message, "Content-Type") {
		t.Errorf("Message: got %q, want it to name the canonical form %q", cmdErr.Message, "Content-Type")
	}
}

func TestApplyCreateSchedule_RejectsReservedSchedulerHeaders(t *testing.T) {
	for _, key := range []string{"X-Scheduler-Idempotency-Key", "X-Scheduler-Attempt-Id"} {
		t.Run(key, func(t *testing.T) {
			tx := newFakeTx()
			cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
				c.Headers = map[string]string{key: "operator-supplied"}
			})
			_, err := ApplyCreateSchedule(tx, *cmd, testTime)
			var cmdErr *CommandError
			if !errors.As(err, &cmdErr) {
				t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
			}
			if cmdErr.Kind != KindValidation {
				t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
			}
			if cmdErr.Field != "headers" {
				t.Errorf("Field: got %q, want %q", cmdErr.Field, "headers")
			}
		})
	}
}

func TestApplyCreateSchedule_RejectsReservedHeaders_Aggregated(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.Headers = map[string]string{
			"Content-Type":           "text/plain",
			"X-Scheduler-Attempt-Id": "operator-supplied",
			"X-Custom-Header":        "fine",
		}
	})
	_, err := ApplyCreateSchedule(tx, *cmd, testTime)
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if !strings.Contains(cmdErr.Message, "Content-Type") || !strings.Contains(cmdErr.Message, "X-Scheduler-Attempt-Id") {
		t.Errorf("Message: got %q, want it to name both offending keys", cmdErr.Message)
	}
	if strings.Contains(cmdErr.Message, "X-Custom-Header") {
		t.Errorf("Message: got %q, should not mention the non-reserved key", cmdErr.Message)
	}
}

func TestApplyCreateSchedule_NonReservedHeaderCasingPreserved(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.Headers = map[string]string{"x-custom-HEADER": "value"}
	})
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if v, ok := schedule.Headers["x-custom-HEADER"]; !ok || v != "value" {
		t.Errorf("Headers: got %+v, want original casing %q preserved", schedule.Headers, "x-custom-HEADER")
	}
}

func TestApplyUpdateSchedule_RejectsReservedHeader(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) {
		h := map[string]string{"Content-Type": "text/plain"}
		c.Headers = &h
	})
	schedule, err := ApplyUpdateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "headers" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "headers")
	}
}

func TestApplyUpdateSchedule_NilHeadersUnaffectedByReservedCheck(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand() // Headers left nil = don't touch
	schedule, err := ApplyUpdateSchedule(tx, *cmd, testTime)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(schedule.Headers, preSeed.Headers) {
		t.Errorf("Headers: got %+v, want unchanged %+v", schedule.Headers, preSeed.Headers)
	}
}

func TestApplyCreateSchedule_UnsetCallTimeoutStaysNil(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.CallTimeout = nil })
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if schedule.CallTimeout != nil {
		t.Errorf("CallTimeout: got %v, want nil (should track the live default, not bake it in)", *schedule.CallTimeout)
	}
}

func TestApplyCreateSchedule_PreservesExplicitCallTimeout(t *testing.T) {
	tx := newFakeTx()
	want := 10 * time.Second
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.CallTimeout = &want })
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if schedule.CallTimeout == nil || *schedule.CallTimeout != want {
		t.Errorf("CallTimeout: got %v, want %v", schedule.CallTimeout, want)
	}
}

func TestApplyCreateSchedule_RejectsCallTimeoutBelowMin(t *testing.T) {
	tx := newFakeTx()
	tooSmall := model.MinCallTimeout - time.Millisecond
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.CallTimeout = &tooSmall })
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "call_timeout" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "call_timeout")
	}
}

func TestApplyCreateSchedule_RejectsCallTimeoutAboveMax(t *testing.T) {
	tx := newFakeTx()
	tooBig := model.MaxCallTimeout + time.Millisecond
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.CallTimeout = &tooBig })
	schedule, err := ApplyCreateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "call_timeout" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "call_timeout")
	}
}

func TestApplyCreateSchedule_RecurrenceComputeFails(t *testing.T) {
	tx := newFakeTx()
	seconds := 3600
	rec := &model.Recurrence{
		Cadence: model.CadenceInterval,
		Seconds: &seconds,
	}
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) {
		c.ScheduleType = model.ScheduleTypeRecurring
		c.Recurrence = rec
		c.FirstRunAt = testTime
		c.Timezone = "Not/A/Real/Zone"
	})
	schedule, err := ApplyCreateSchedule(tx, *cmd, proposedAt)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "timezone" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "timezone")
	}
}

func TestApplyCreateSchedule_MissingID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ID = "" })
	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyCreateSchedule_IDCollision(t *testing.T) {
	tx := newFakeTx()
	existing := &model.Schedule{
		TenantID: testTenantID,
		ID:       testScheduleID,
	}
	tx.schedules[testTenantID+":"+testScheduleID] = existing
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ID = existing.ID })
	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyCreateSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.TenantID = "" })

	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyUpdateSchedule_HappyPathOneField(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	newURL := "https://new.example.com/hook"
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallbackURL = &newURL })
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyUpdateSchedule: returned nil schedule with nil error")
	}
	if schedule.CallbackURL != newURL {
		t.Errorf("CallbackURL: got %q, want %q", schedule.CallbackURL, newURL)
	}
	if !reflect.DeepEqual(schedule.Headers, preSeed.Headers) {
		t.Errorf("Headers: got %+v, want %+v", schedule.Headers, preSeed.Headers)
	}
	if !reflect.DeepEqual(schedule.Payload, preSeed.Payload) {
		t.Errorf("Payload: got %v, want %v", schedule.Payload, preSeed.Payload)
	}
	if schedule.RetryBackoff != preSeed.RetryBackoff {
		t.Errorf("RetryBackoff: got %+v, want %+v", schedule.RetryBackoff, preSeed.RetryBackoff)
	}
	if schedule.MaxAttempts != preSeed.MaxAttempts {
		t.Errorf("MaxAttempts: got %d, want %d", schedule.MaxAttempts, preSeed.MaxAttempts)
	}
	if !reflect.DeepEqual(schedule.CallTimeout, preSeed.CallTimeout) {
		t.Errorf("CallTimeout: got %v, want %v", schedule.CallTimeout, preSeed.CallTimeout)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if !schedule.CreatedAt.Equal(preSeed.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, preSeed.CreatedAt)
	}

	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.CallbackURL != newURL {
		t.Errorf("stored.CallbackURL: got %q, want %q", stored.CallbackURL, newURL)
	}
}

func TestApplyUpdateSchedule_UpdatesCallTimeout(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	want := 45 * time.Second
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallTimeout = durationPtr(want) })
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule.CallTimeout == nil || *schedule.CallTimeout != want {
		t.Errorf("CallTimeout: got %v, want %v", schedule.CallTimeout, want)
	}
}

func TestApplyUpdateSchedule_ResetsCallTimeoutToNil(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule() // has a concrete, non-nil CallTimeout
	seedSchedule(tx, preSeed)
	zero := time.Duration(0)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallTimeout = &zero })
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule.CallTimeout != nil {
		t.Errorf("CallTimeout: got %v, want nil — an explicit 0 must reset to tracking the live default", *schedule.CallTimeout)
	}
}

func TestApplyUpdateSchedule_RejectsCallTimeoutBelowMin(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	tooSmall := model.MinCallTimeout - time.Millisecond
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallTimeout = &tooSmall })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "call_timeout" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "call_timeout")
	}
}

func TestApplyUpdateSchedule_RejectsCallTimeoutAboveMax(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	tooBig := model.MaxCallTimeout + time.Millisecond
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallTimeout = &tooBig })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, testTime)
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "call_timeout" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "call_timeout")
	}
}

func TestApplyUpdateSchedule_HappyPathNoFields(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand()
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyUpdateSchedule: returned nil schedule with nil error")
	}
	if schedule.CallbackURL != preSeed.CallbackURL {
		t.Errorf("CallbackURL: got %q, want %q", schedule.CallbackURL, preSeed.CallbackURL)
	}
	if !reflect.DeepEqual(schedule.Headers, preSeed.Headers) {
		t.Errorf("Headers: got %+v, want %+v", schedule.Headers, preSeed.Headers)
	}
	if !reflect.DeepEqual(schedule.Payload, preSeed.Payload) {
		t.Errorf("Payload: got %v, want %v", schedule.Payload, preSeed.Payload)
	}
	if schedule.RetryBackoff != preSeed.RetryBackoff {
		t.Errorf("RetryBackoff: got %+v, want %+v", schedule.RetryBackoff, preSeed.RetryBackoff)
	}
	if schedule.MaxAttempts != preSeed.MaxAttempts {
		t.Errorf("MaxAttempts: got %d, want %d", schedule.MaxAttempts, preSeed.MaxAttempts)
	}
	if !reflect.DeepEqual(schedule.CallTimeout, preSeed.CallTimeout) {
		t.Errorf("CallTimeout: got %v, want %v", schedule.CallTimeout, preSeed.CallTimeout)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if !schedule.CreatedAt.Equal(preSeed.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, preSeed.CreatedAt)
	}

	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.CallbackURL != preSeed.CallbackURL {
		t.Errorf("stored.CallbackURL: got %q, want %q", stored.CallbackURL, preSeed.CallbackURL)
	}
}

func TestApplyUpdateSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.TenantID = "" })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyUpdateSchedule_MissingID(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.ID = "" })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}

}

func TestApplyUpdateSchedule_NotFound(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestUpdateScheduleCommand()
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindNotFound {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindNotFound)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyUpdateSchedule_CanceledSchedule(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCanceled
	})
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand()
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "status" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "status")
	}

}

func TestApplyPauseSchedule_HappyPath(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestPauseScheduleCommand()
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyPauseSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyPauseSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyPauseSchedule: returned nil schedule with nil error")
	}
	if schedule.Status != model.ScheduleStatusPaused {
		t.Errorf("Schedule did not pause correctly")
	}
	if schedule.NextRunAt != nil {
		t.Error("NextRunAt is not nil")
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Error("UpdatedAt did not update")
	}
}

func TestApplyPauseSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestPauseScheduleCommand(func(c *PauseScheduleCommand) { c.TenantID = "" })
	schedule, err := ApplyPauseSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyPauseSchedule_NotFound(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestPauseScheduleCommand()
	schedule, err := ApplyPauseSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindNotFound {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindNotFound)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyPauseSchedule_CanceledSchedule(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCanceled
	})
	seedSchedule(tx, preSeed)
	cmd := newTestPauseScheduleCommand()
	schedule, err := ApplyPauseSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "status" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "status")
	}

}

func TestApplyCancelSchedule_HappyPath(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestCancelScheduleCommand()
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyCancelSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyCancelSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyCancelSchedule: returned nil schedule with nil error")
	}
	if schedule.Status != model.ScheduleStatusCanceled {
		t.Errorf("Schedule did not pause correctly")
	}
	if schedule.NextRunAt != nil {
		t.Error("NextRunAt is not nil")
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
}

func TestApplyCancelSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCancelScheduleCommand(func(c *CancelScheduleCommand) { c.TenantID = "" })
	schedule, err := ApplyCancelSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyCancelSchedule_NotFound(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCancelScheduleCommand()
	schedule, err := ApplyCancelSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindNotFound {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindNotFound)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyCancelSchedule_AlreadyCanceled(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCanceled
	})
	seedSchedule(tx, preSeed)
	cmd := newTestCancelScheduleCommand()
	schedule, err := ApplyCancelSchedule(tx, *cmd, time.Time{})
	if err != nil {
		t.Fatalf("expected success on already-canceled, got error: %v", err)
	}
	if schedule == nil {
		t.Fatal("returned nil schedule with nil error")
	}
	if schedule.Status != model.ScheduleStatusCanceled {
		t.Errorf("Status: got %q, want %q", schedule.Status, model.ScheduleStatusCanceled)
	}

}

func TestApplyResumeSchedule_HappyPathFromPaused(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.CatchUpPolicy = model.CatchUpPolicySkipMissed
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}
	expected := time.Date(2026, 7, 21, 13, 0, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(expected) {
		t.Errorf("NextRunAt: got %v, want %v", *got.NextRunAt, expected)
	}
	if !got.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, proposedAt)
	}
}

func TestApplyResumeSchedule_HappyPathCatchUpAll(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule()
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	ex := newTestExecution()
	seedExecution(tx, ex)
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}
	expected := time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(expected) {
		t.Errorf("NextRunAt: got %v, want %v", *got.NextRunAt, expected)
	}
	if !got.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, proposedAt)
	}
}

func TestApplyResumeSchedule_HappyPathCatchUpLatestOnly(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.CatchUpPolicy = model.CatchUpPolicyLatestOnly
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}
	expected := time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(expected) {
		t.Errorf("NextRunAt: got %v, want %v", *got.NextRunAt, expected)
	}
	if !got.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, proposedAt)
	}
}

func TestApplyResumeSchedule_HappyPathOnceSchedule(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.ScheduleType = model.ScheduleTypeOnce
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}
	expected := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(expected) {
		t.Errorf("NextRunAt: got %v, want %v", *got.NextRunAt, expected)
	}
	if !got.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, proposedAt)
	}
}

func TestApplyResumeSchedule_HappyPathNoPriorExecutions(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule()
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}
	expected := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if !got.NextRunAt.Equal(expected) {
		t.Errorf("NextRunAt: got %v, want %v", *got.NextRunAt, expected)
	}
	if !got.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", got.UpdatedAt, proposedAt)
	}
}

func TestApplyResumeSchedule_AlreadyActive(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusActive
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil schedule")
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if got.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", got.Status, model.ScheduleStatusActive)
	}
	if !got.UpdatedAt.Equal(schedule.UpdatedAt) {
		t.Errorf("UpdatedAt was modified: got %v, want %v", got.UpdatedAt, schedule.UpdatedAt)
	}
	if got.NextRunAt != schedule.NextRunAt { // both should be nil for the seeded paused/active-override case
		t.Errorf("NextRunAt was modified: got %v, want %v", got.NextRunAt, schedule.NextRunAt)
	}
}

func TestApplyResumeSchedule_CanceledSchedule(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCanceled
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if got != nil {
		t.Errorf("expected nil schedule on error, got %+v", got)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "status" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "status")
	}
}

func TestApplyResumeSchedule_CompletedSchedule(t *testing.T) {
	tx := newFakeTx()
	proposedAt := testTime
	schedule := newTestPausedSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCompleted
	})
	seedSchedule(tx, schedule)
	cmd := newTestResumeScheduleCommand()
	got, err := ApplyResumeSchedule(tx, *cmd, proposedAt)
	if got != nil {
		t.Errorf("expected nil schedule on error, got %+v", got)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "status" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "status")
	}
}
