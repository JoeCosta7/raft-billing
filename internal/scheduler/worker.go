package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"raft-biling/internal/command"
	"raft-biling/internal/model"
	"runtime/debug"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type Bucket string

const (
	BucketNoAttempts      Bucket = "no_attempts"
	BucketSuccess         Bucket = "success"
	BucketTerminalFailure Bucket = "terminal_failure"
	BucketRetryElapsed    Bucket = "retry_elapsed"
	BucketRetryPending    Bucket = "retry_pending"
)

// proposeTimeout bounds how long a single recovery-driven Propose call waits.
const proposeTimeout = 5 * time.Second

// TODO defaultPoolSize value needs to be figured out
const defaultPoolSize = 8

// TODO defaultTickInterval value needs to be figured out
const defaultTickInterval = 5 * time.Second

// responseExcerptLimit bounds how much of a callback response body is retained on an Attempt.
const responseExcerptLimit = 4096

type Task interface {
	run(ctx context.Context) error
}

type inFlightTaskKind string

const (
	KindRetry    inFlightTaskKind = "retry"
	KindTimeout  inFlightTaskKind = "timeout"
	KindFinalize inFlightTaskKind = "finalize"
)

type inFlightTask struct {
	exec               model.Execution
	schedule           *model.Schedule
	reader             Reader
	kind               inFlightTaskKind
	proposer           Proposer
	httpClient         *http.Client
	logger             *slog.Logger
	defaultCallTimeout time.Duration
}

type freshFireTask struct {
	schedule           *model.Schedule
	exec               model.Execution
	nodeID             string
	reader             Reader
	proposer           Proposer
	httpClient         *http.Client
	logger             *slog.Logger
	defaultCallTimeout time.Duration
}

func asCommandError(result any) *command.CommandError {
	cmdErr, _ := result.(*command.CommandError)
	return cmdErr
}

// 2XX = success
func classifyAttemptOutcome(status int, doErr error, attemptNumber, maxAttempts int) model.AttemptOutcome {
	if doErr == nil && status >= 200 && status < 300 {
		return model.OutcomeSuccess
	}
	if maxAttempts > 0 && attemptNumber >= maxAttempts {
		return model.OutcomeTerminalFailure
	}
	return model.OutcomeRetry
}

func flattenHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func classifyOne(exec model.Execution, attempts []*model.Attempt, now time.Time) (Bucket, error) {
	if len(attempts) == 0 {
		return BucketNoAttempts, nil
	}
	switch attempts[len(attempts)-1].Outcome {
	case model.OutcomeSuccess:
		return BucketSuccess, nil
	case model.OutcomeTerminalFailure:
		return BucketTerminalFailure, nil
	case model.OutcomeRetry:
		if !now.Before(attempts[len(attempts)-1].RetryAt) {
			return BucketRetryElapsed, nil
		}
		return BucketRetryPending, nil
	default:
		return "", fmt.Errorf("unknown outcome value: %v", attempts[len(attempts)-1].Outcome)
	}
}

type Worker struct {
	reader             Reader
	proposer           Proposer
	nodeID             string
	logger             *slog.Logger
	taskCh             chan Task
	wg                 sync.WaitGroup
	httpClient         *http.Client
	tickInterval       time.Duration
	poolSize           int
	defaultCallTimeout time.Duration
}

func NewWorker(reader Reader, proposer Proposer, logger *slog.Logger) *Worker {
	return &Worker{
		reader:             reader,
		proposer:           proposer,
		nodeID:             proposer.ID(),
		logger:             logger,
		taskCh:             make(chan Task, 256),
		httpClient:         &http.Client{},
		tickInterval:       defaultTickInterval,
		poolSize:           defaultPoolSize,
		defaultCallTimeout: model.DefaultCallTimeout,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.runRecover(ctx); err != nil {
		return err
	}

	var poolWg sync.WaitGroup
	poolWg.Add(w.poolSize)
	for range w.poolSize {
		go func() {
			defer poolWg.Done()
			w.runPool(ctx)
		}()
	}

	err := w.steadyState(ctx)
	close(w.taskCh)
	poolWg.Wait()
	return err
}

func (w *Worker) runPool(ctx context.Context) {
	for task := range w.taskCh {
		w.runTask(ctx, task)
	}
}

func (w *Worker) runTask(ctx context.Context, task Task) {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("task panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	if err := task.run(ctx); err != nil {
		w.logger.Error("task failed", "err", err)
	}
}

func (t *freshFireTask) run(ctx context.Context) error {
	execID := t.exec.ID
	if err := ctx.Err(); err != nil {
		return err
	}
	schedule, err := t.reader.GetSchedule(t.exec.TenantID, t.exec.ScheduleID)
	if err != nil {
		t.logger.Error("fresh-fire schedule read failed",
			"execution", execID, "schedule", t.exec.ScheduleID, "err", err)
		return fmt.Errorf("get schedule for execution %s: %w", execID, err)
	}
	if schedule == nil {
		t.logger.Error("fresh-fire schedule not found",
			"execution", execID, "schedule", t.exec.ScheduleID)
		return nil
	}
	if schedule.Status != model.ScheduleStatusActive {
		t.logger.Warn("skipping fresh-fire dispatch: schedule not active — execution remains in-flight with no attempt recorded; no automatic mechanism currently resolves this orphan",
			"execution", execID, "schedule", schedule.ID, "status", schedule.Status)
		return nil
	}
	t.schedule = schedule
	attemptID := ulid.Make().String()
	startedAt := time.Now()
	body := t.schedule.Payload
	callTimeout := t.defaultCallTimeout
	if t.schedule.CallTimeout != nil {
		callTimeout = *t.schedule.CallTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, t.schedule.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request for execution %s: %w", execID, err)
	}
	for k, v := range t.schedule.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	bodyHash := sha256.Sum256(body)
	resp, doErr := t.httpClient.Do(req)
	if errors.Is(doErr, context.Canceled) {
		// workerCtx was canceled mid-dispatch (shutdown or explicit task cancellation).
		// No attempt outcome to record — DeadlineExceeded is intentionally NOT caught
		// here: that means CallTimeout fired on a slow receiver, which is a real
		// attempt outcome that must still be classified, recorded, and retried.
		return ctx.Err()
	}
	completedAt := time.Now()
	var respStatus int
	var respHeaders map[string]string
	var respExcerpt string
	if resp != nil {
		defer resp.Body.Close()
		respStatus = resp.StatusCode
		respHeaders = flattenHeaders(resp.Header)

		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, responseExcerptLimit))
		respExcerpt = string(excerpt)
		if readErr != nil {
			t.logger.Warn("failed to fully read callback response body",
				"execution", execID, "err", readErr)
		}
	}
	const attemptNumber = 1
	outcome := classifyAttemptOutcome(respStatus, doErr, attemptNumber, t.schedule.MaxAttempts)
	var retryAt *time.Time
	if outcome == model.OutcomeRetry {
		at := completedAt.Add(t.schedule.RetryBackoff.DelayForAttempt(attemptNumber))
		retryAt = &at
	}
	recordCmd := command.RecordAttemptCommand{
		TenantID:            t.schedule.TenantID,
		ExecutionID:         execID,
		ID:                  attemptID,
		NodeID:              t.nodeID,
		StartedAt:           &startedAt,
		CompletedAt:         &completedAt,
		Outcome:             outcome,
		RequestURL:          t.schedule.CallbackURL,
		RequestHeaders:      t.schedule.Headers,
		RequestBodyHash:     hex.EncodeToString(bodyHash[:]),
		ResponseStatus:      respStatus,
		ResponseBodyExcerpt: respExcerpt,
		ResponseHeaders:     respHeaders,
		RetryAt:             retryAt,
	}

	result, err := t.proposer.Propose("record_attempt", recordCmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose record attempt for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		return fmt.Errorf("record attempt rejected for execution %s: %w", execID, cmdErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if outcome != model.OutcomeRetry {
		finalStatus := model.ExecutionStatusSucceeded
		if outcome == model.OutcomeTerminalFailure {
			finalStatus = model.ExecutionStatusFailedTerminal
		}
		completeCmd := command.CompleteExecutionCommand{
			TenantID:     t.schedule.TenantID,
			ExecutionID:  execID,
			FinalStatus:  finalStatus,
			FinalOutcome: string(outcome),
		}
		result, err := t.proposer.Propose("complete_execution", completeCmd, proposeTimeout)
		if err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", execID, err)
		}
		if cmdErr := asCommandError(result); cmdErr != nil {
			return fmt.Errorf("complete execution rejected for execution %s: %w", execID, cmdErr)
		}
	}

	return nil
}

func (t *inFlightTask) run(ctx context.Context) error {
	execID := t.exec.ID

	if err := ctx.Err(); err != nil {
		return err
	}

	switch t.kind {
	case KindRetry:
		return t.runRetry(ctx, execID)
	case KindTimeout:
		return t.runTimeout(execID)
	case KindFinalize:
		return t.runFinalize(execID)
	default:
		return fmt.Errorf("inFlightTask: unhandled kind %q for execution %s", t.kind, execID)
	}
}

func (t *inFlightTask) runFinalize(execID string) error {
	attempts, err := t.reader.ListAttemptsByExecution(t.exec.TenantID, execID)
	if err != nil {
		return fmt.Errorf("list attempts for execution %s: %w", execID, err)
	}
	if len(attempts) == 0 {
		t.logger.Error("finalize task found no attempts for execution with non-empty LastAttemptID",
			"execution", execID)
		return nil
	}
	lastOutcome := attempts[len(attempts)-1].Outcome

	var finalStatus model.ExecutionStatus
	var finalOutcome string
	switch lastOutcome {
	case model.OutcomeSuccess:
		finalStatus = model.ExecutionStatusSucceeded
		finalOutcome = string(model.OutcomeSuccess)
	case model.OutcomeTerminalFailure:
		finalStatus = model.ExecutionStatusFailedTerminal
		finalOutcome = string(model.OutcomeTerminalFailure)
	default:
		t.logger.Error("finalize task found non-terminal last attempt outcome",
			"execution", execID, "outcome", lastOutcome)
		return nil
	}

	cmd := command.CompleteExecutionCommand{
		TenantID:     t.exec.TenantID,
		ExecutionID:  execID,
		FinalStatus:  finalStatus,
		FinalOutcome: finalOutcome,
	}
	result, err := t.proposer.Propose("complete_execution", cmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose complete for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		if cmdErr.Kind == command.KindConflict {
			t.logger.Info("execution already finalized elsewhere", "execution", execID)
			return nil
		}
		return fmt.Errorf("complete execution rejected for execution %s: %w", execID, cmdErr)
	}
	return nil
}

func (t *inFlightTask) runTimeout(execID string) error {
	cmd := command.FailExecutionTimeoutCommand{
		TenantID:     t.exec.TenantID,
		ExecutionID:  execID,
		FinalOutcome: model.FinalOutcomeTimedOut,
	}
	result, err := t.proposer.Propose("fail_execution_timeout", cmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose fail execution timeout for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		if cmdErr.Kind == command.KindConflict {
			t.logger.Info("execution already terminal, timeout not applied",
				"execution", execID, "err", cmdErr)
			return nil
		}
		return fmt.Errorf("fail execution timeout rejected for execution %s: %w", execID, cmdErr)
	}
	return nil
}

func (t *inFlightTask) runRetry(ctx context.Context, execID string) error {
	schedule, err := t.reader.GetSchedule(t.exec.TenantID, t.exec.ScheduleID)
	if err != nil {
		t.logger.Error("retry schedule read failed",
			"execution", execID, "schedule", t.exec.ScheduleID, "err", err)
		return fmt.Errorf("get schedule for execution %s: %w", execID, err)
	}
	if schedule == nil {
		t.logger.Error("retry schedule not found",
			"execution", execID, "schedule", t.exec.ScheduleID)
		return nil
	}
	if schedule.Status != model.ScheduleStatusActive {
		t.logger.Warn("skipping retry dispatch: schedule not active",
			"execution", execID, "schedule", schedule.ID, "status", schedule.Status)
		return nil
	}
	t.schedule = schedule
	attemptID := ulid.Make().String()
	startedAt := time.Now()
	body := t.schedule.Payload
	callTimeout := t.defaultCallTimeout
	if t.schedule.CallTimeout != nil {
		callTimeout = *t.schedule.CallTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, t.schedule.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request for execution %s: %w", execID, err)
	}
	for k, v := range t.schedule.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Content-Type", "application/json")
	bodyHash := sha256.Sum256(body)
	resp, doErr := t.httpClient.Do(req)
	if errors.Is(doErr, context.Canceled) {
		// workerCtx was canceled mid-dispatch (shutdown or explicit task cancellation).
		// No attempt outcome to record — DeadlineExceeded is intentionally NOT caught
		// here: that means CallTimeout fired on a slow receiver, which is a real
		// attempt outcome that must still be classified, recorded, and retried.
		return ctx.Err()
	}
	completedAt := time.Now()
	var respStatus int
	var respHeaders map[string]string
	var respExcerpt string
	if resp != nil {
		defer resp.Body.Close()
		respStatus = resp.StatusCode
		respHeaders = flattenHeaders(resp.Header)

		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, responseExcerptLimit))
		respExcerpt = string(excerpt)
		if readErr != nil {
			t.logger.Warn("failed to fully read callback response body",
				"execution", execID, "err", readErr)
		}
	}

	attemptNumber := t.exec.AttemptCount + 1
	outcome := classifyAttemptOutcome(respStatus, doErr, attemptNumber, t.schedule.MaxAttempts)
	var retryAt *time.Time
	if outcome == model.OutcomeRetry {
		at := completedAt.Add(t.schedule.RetryBackoff.DelayForAttempt(attemptNumber))
		retryAt = &at
	}
	recordCmd := command.RecordAttemptCommand{
		TenantID:            t.schedule.TenantID,
		ExecutionID:         execID,
		ID:                  attemptID,
		NodeID:              t.proposer.ID(),
		StartedAt:           &startedAt,
		CompletedAt:         &completedAt,
		Outcome:             outcome,
		RequestURL:          t.schedule.CallbackURL,
		RequestHeaders:      t.schedule.Headers,
		RequestBodyHash:     hex.EncodeToString(bodyHash[:]),
		ResponseStatus:      respStatus,
		ResponseBodyExcerpt: respExcerpt,
		ResponseHeaders:     respHeaders,
		RetryAt:             retryAt,
	}

	result, err := t.proposer.Propose("record_attempt", recordCmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose record attempt for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		return fmt.Errorf("record attempt rejected for execution %s: %w", execID, cmdErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if outcome != model.OutcomeRetry {
		finalStatus := model.ExecutionStatusSucceeded
		if outcome == model.OutcomeTerminalFailure {
			finalStatus = model.ExecutionStatusFailedTerminal
		}
		completeCmd := command.CompleteExecutionCommand{
			TenantID:     t.schedule.TenantID,
			ExecutionID:  execID,
			FinalStatus:  finalStatus,
			FinalOutcome: string(outcome),
		}
		result, err := t.proposer.Propose("complete_execution", completeCmd, proposeTimeout)
		if err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", execID, err)
		}
		if cmdErr := asCommandError(result); cmdErr != nil {
			return fmt.Errorf("complete execution rejected for execution %s: %w", execID, cmdErr)
		}
	}

	return nil
}

func (w *Worker) runRecover(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: panic during recovery: %v\n%s", r, debug.Stack())
		}
	}()

	tenants, err := w.reader.ListTenants()
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}

	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return err
		}
		execs, err := w.reader.ListExecutionsByStatus(tenant.ID, model.ExecutionStatusInFlight)
		if err != nil {
			return fmt.Errorf("list in-flight for tenant %s: %w", tenant.ID, err)
		}

		for _, exec := range execs {
			if err := ctx.Err(); err != nil {
				return err
			}
			attempts, err := w.reader.ListAttemptsByExecution(tenant.ID, exec.ID)
			if err != nil {
				return fmt.Errorf("read attempts for execution %s: %w", exec.ID, err)
			}

			bucket, err := classifyOne(*exec, attempts, time.Now())
			if err != nil {
				return fmt.Errorf("classify for execution %s: %w", exec.ID, err)
			}

			if err := w.adoptOrComplete(ctx, *tenant, *exec, bucket); err != nil {
				return fmt.Errorf("adopt or complete for execution %s: %w", exec.ID, err)
			}
		}
	}
	return nil
}

func (w *Worker) adoptOrComplete(ctx context.Context, tenant model.Tenant, exec model.Execution, bucket Bucket) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch bucket {
	case BucketNoAttempts, BucketRetryElapsed, BucketRetryPending:
		cmd := command.AdoptExecutionCommand{
			TenantID:      tenant.ID,
			ExecutionID:   exec.ID,
			PreviousOwner: exec.OwnerNodeID,
			NewOwner:      w.nodeID,
		}
		if _, err := w.proposer.Propose("adopt_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose adopt for execution %s: %w", exec.ID, err)
		}
		return nil
	case BucketSuccess:
		cmd := command.CompleteExecutionCommand{
			TenantID:     tenant.ID,
			ExecutionID:  exec.ID,
			FinalStatus:  model.ExecutionStatusSucceeded,
			FinalOutcome: string(model.OutcomeSuccess),
		}
		if _, err := w.proposer.Propose("complete_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", exec.ID, err)
		}
		return nil
	case BucketTerminalFailure:
		cmd := command.CompleteExecutionCommand{
			TenantID:     tenant.ID,
			ExecutionID:  exec.ID,
			FinalStatus:  model.ExecutionStatusFailedTerminal,
			FinalOutcome: string(model.OutcomeTerminalFailure),
		}
		if _, err := w.proposer.Propose("complete_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", exec.ID, err)
		}
		return nil
	default:
		return fmt.Errorf("adoptOrComplete: unhandled bucket %q", bucket)
	}
}

func (w *Worker) steadyState(ctx context.Context) error {
	ticker := time.NewTicker(w.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.runTick(ctx); err != nil {
				return fmt.Errorf("run tick: %w", err)
			}
		}
	}
}

func (w *Worker) runTick(ctx context.Context) error {
	tenants, err := w.reader.ListTenants()
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}
	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return err
		}
		//Schedule-scan
		//TODO Implement ListSchedulesDue
		schedules, err := w.reader.ListSchedulesDue(tenant.ID)
		if err != nil {
			w.logger.Warn("schedule scan failed", "tenant", tenant.ID, "err", err)
			continue
		}
		for _, schedule := range schedules {
			if err := ctx.Err(); err != nil {
				return err
			}
			if schedule.NextRunAt == nil {
				w.logger.Error("due schedule has nil NextRunAt — index invariant violated",
					"tenant", tenant.ID, "schedule", schedule.ID)
				continue
			}
			scheduledFor := *schedule.NextRunAt
			claimCmd := command.ClaimExecutionCommand{
				ID:           ulid.Make().String(),
				TenantID:     tenant.ID,
				ScheduleID:   schedule.ID,
				ScheduledFor: scheduledFor,
				OwnerNodeID:  w.nodeID,
			}
			result, err := w.proposer.Propose("claim_execution", claimCmd, proposeTimeout)
			if err != nil {
				w.logger.Warn("propose claim execution failed",
					"tenant", tenant.ID, "schedule", schedule.ID, "execution", claimCmd.ID, "err", err)
				continue
			}
			if cmdErr := asCommandError(result); cmdErr != nil {
				if cmdErr.Kind == command.KindConflict {
					w.logger.Info("execution already claimed for schedule occurrence",
						"tenant", tenant.ID, "schedule", schedule.ID, "scheduled_for", scheduledFor)
				} else {
					w.logger.Warn("claim execution rejected",
						"tenant", tenant.ID, "schedule", schedule.ID, "execution", claimCmd.ID, "err", cmdErr)
				}
				continue
			}
			claimedExec, ok := result.(*model.Execution)
			if !ok || claimedExec == nil {
				w.logger.Error("claim execution returned unexpected result",
					"tenant", tenant.ID, "schedule", schedule.ID)
				continue
			}
			dispatch := &freshFireTask{
				schedule:           schedule,
				exec:               *claimedExec,
				nodeID:             w.nodeID,
				reader:             w.reader,
				proposer:           w.proposer,
				httpClient:         w.httpClient,
				logger:             w.logger,
				defaultCallTimeout: w.defaultCallTimeout,
			}
			w.wg.Add(1)
			w.taskCh <- dispatch
		}
		//in-flight-scan
		executions, err := w.reader.ListExecutionsByStatus(tenant.ID, model.ExecutionStatusInFlight)
		if err != nil {
			w.logger.Warn("list executions by status for tenant", "tenant", tenant.ID, "err", err)
			continue
		}
		for _, exec := range executions {
			if err := ctx.Err(); err != nil {
				return err
			}
			if exec.LastAttemptID != "" && exec.LastRetryAt == nil {
				finalize := &inFlightTask{
					exec:     *exec,
					reader:   w.reader,
					kind:     KindFinalize,
					proposer: w.proposer,
					logger:   w.logger,
				}
				w.wg.Add(1)
				w.taskCh <- finalize
				continue
			}
			if exec.LastRetryAt != nil && !exec.LastRetryAt.After(time.Now()) {
				schedule, err := w.reader.GetSchedule(tenant.ID, exec.ScheduleID)
				if err != nil {
					w.logger.Warn("parent schedule load failed",
						"tenant", tenant.ID, "execution", exec.ID, "schedule", exec.ScheduleID, "err", err)
					continue // skip this Execution; try again next tick
				}
				executeRetry := &inFlightTask{
					exec:               *exec,
					schedule:           schedule,
					reader:             w.reader,
					kind:               KindRetry,
					proposer:           w.proposer,
					httpClient:         w.httpClient,
					logger:             w.logger,
					defaultCallTimeout: w.defaultCallTimeout,
				}
				w.wg.Add(1)
				w.taskCh <- executeRetry
			}
		}
	}
	w.wg.Wait()
	return nil
}
