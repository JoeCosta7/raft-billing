package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
	"time"
)

// proposeTimeout bounds how long a single HTTP-triggered Propose call waits.
const proposeTimeout = 5 * time.Second

type API struct {
	cfg     *config.Config
	backend Backend
	logger  *slog.Logger
	server  *http.Server
}

func New(cfg *config.Config, rn *raftnode.RaftNode, sm *statemachine.StateMachine) *API {
	a := &API{cfg: cfg, backend: rn, logger: slog.Default()}
	a.server = &http.Server{Handler: a.routes()}
	return a
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tenants", a.handleCreateTenant)
	mux.HandleFunc("GET /tenants", a.handleListTenants)
	mux.HandleFunc("GET /tenants/{tenantID}", a.handleGetTenant)

	mux.HandleFunc("POST /tenants/{tenantID}/schedules", a.handleCreateSchedule)
	mux.HandleFunc("GET /tenants/{tenantID}/schedules/{scheduleID}", a.handleGetSchedule)
	mux.HandleFunc("POST /tenants/{tenantID}/schedules/{scheduleID}/pause", a.handlePauseSchedule)
	mux.HandleFunc("POST /tenants/{tenantID}/schedules/{scheduleID}/cancel", a.handleCancelSchedule)
	mux.HandleFunc("POST /tenants/{tenantID}/schedules/{scheduleID}/resume", a.handleResumeSchedule)
	mux.HandleFunc("GET /tenants/{tenantID}/schedules/{scheduleID}/executions", a.handleListExecutionsBySchedule)

	mux.HandleFunc("GET /tenants/{tenantID}/executions", a.handleListExecutionsByStatus)
	mux.HandleFunc("GET /tenants/{tenantID}/executions/{executionID}", a.handleGetExecution)
	mux.HandleFunc("GET /tenants/{tenantID}/executions/{executionID}/attempts", a.handleListAttemptsByExecution)

	mux.HandleFunc("GET /tenants/{tenantID}/attempts/{attemptID}", a.handleGetAttempt)
	return mux
}

func (a *API) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", a.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("api listen on %s: %w", a.cfg.HTTPAddr, err)
	}
	go func() {
		if err := a.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("api server exited with error", "error", err)
		}
	}()
	return nil
}

func (a *API) Shutdown(ctx context.Context) error {
	return a.server.Shutdown(ctx)
}

// ---- response helpers ----

type errorBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

// requireLeader answers the request itself (503, with the current raft
// leader address as a hint) and returns false when this node isn't leader.
func (a *API) requireLeader(w http.ResponseWriter) bool {
	if a.backend.IsLeader() {
		return true
	}
	writeError(w, http.StatusServiceUnavailable,
		fmt.Sprintf("not leader; current raft leader address: %s", a.backend.LeaderAddr()))
	return false
}

func asCommandError(result any) *command.CommandError {
	cmdErr, _ := result.(*command.CommandError)
	return cmdErr
}

func writeReadError(w http.ResponseWriter, err error) {
	if errors.Is(err, raftnode.ErrNotCaughtUp) {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func statusForCommandErrorKind(kind string) int {
	switch kind {
	case command.KindValidation:
		return http.StatusBadRequest
	case command.KindNotFound:
		return http.StatusNotFound
	case command.KindConflict:
		return http.StatusConflict
	default: // command.KindStorage and anything unrecognized
		return http.StatusInternalServerError
	}
}

// propose is the shared write path: leader-gate, propose, and translate
// either a transport error or a rejected CommandError into a response. It
// writes the response itself on failure;
func (a *API) propose(w http.ResponseWriter, cmdType string, cmd any) (result any, ok bool) {
	if !a.requireLeader(w) {
		return nil, false
	}
	result, err := a.backend.Propose(cmdType, cmd, proposeTimeout)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return nil, false
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		writeError(w, statusForCommandErrorKind(cmdErr.Kind), cmdErr.Message)
		return nil, false
	}
	return result, true
}

// ---- write handlers ----

func (a *API) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var cmd command.CreateTenantCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	if result, ok := a.propose(w, "create_tenant", cmd); ok {
		writeJSON(w, http.StatusCreated, result)
	}
}

func (a *API) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	var cmd command.CreateScheduleCommand
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}
	cmd.TenantID = r.PathValue("tenantID")
	if result, ok := a.propose(w, "create_schedule", cmd); ok {
		writeJSON(w, http.StatusCreated, result)
	}
}

func (a *API) handlePauseSchedule(w http.ResponseWriter, r *http.Request) {
	cmd := command.PauseScheduleCommand{TenantID: r.PathValue("tenantID"), ID: r.PathValue("scheduleID")}
	if result, ok := a.propose(w, "pause_schedule", cmd); ok {
		writeJSON(w, http.StatusOK, result)
	}
}

func (a *API) handleCancelSchedule(w http.ResponseWriter, r *http.Request) {
	cmd := command.CancelScheduleCommand{TenantID: r.PathValue("tenantID"), ID: r.PathValue("scheduleID")}
	if result, ok := a.propose(w, "cancel_schedule", cmd); ok {
		writeJSON(w, http.StatusOK, result)
	}
}

func (a *API) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	cmd := command.ResumeScheduleCommand{TenantID: r.PathValue("tenantID"), ID: r.PathValue("scheduleID")}
	if result, ok := a.propose(w, "resume_schedule", cmd); ok {
		writeJSON(w, http.StatusOK, result)
	}
}

// ---- read handlers ----

func (a *API) handleListTenants(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	tenants, err := a.backend.ListTenants()
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenants)
}

func (a *API) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	tenant, err := a.backend.GetTenant(r.PathValue("tenantID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	if tenant == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	writeJSON(w, http.StatusOK, tenant)
}

func (a *API) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	schedule, err := a.backend.GetSchedule(r.PathValue("tenantID"), r.PathValue("scheduleID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	if schedule == nil {
		writeError(w, http.StatusNotFound, "schedule not found")
		return
	}
	writeJSON(w, http.StatusOK, schedule)
}

func (a *API) handleListExecutionsBySchedule(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	executions, err := a.backend.ListExecutionsBySchedule(r.PathValue("tenantID"), r.PathValue("scheduleID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, executions)
}

func (a *API) handleListExecutionsByStatus(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		writeError(w, http.StatusBadRequest, "status query parameter is required")
		return
	}
	executions, err := a.backend.ListExecutionsByStatus(r.PathValue("tenantID"), model.ExecutionStatus(status))
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, executions)
}

func (a *API) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	execution, err := a.backend.GetExecution(r.PathValue("tenantID"), r.PathValue("executionID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	if execution == nil {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	writeJSON(w, http.StatusOK, execution)
}

func (a *API) handleListAttemptsByExecution(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	attempts, err := a.backend.ListAttemptsByExecution(r.PathValue("tenantID"), r.PathValue("executionID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, attempts)
}

func (a *API) handleGetAttempt(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	attempt, err := a.backend.GetAttempt(r.PathValue("tenantID"), r.PathValue("attemptID"))
	if err != nil {
		writeReadError(w, err)
		return
	}
	if attempt == nil {
		writeError(w, http.StatusNotFound, "attempt not found")
		return
	}
	writeJSON(w, http.StatusOK, attempt)
}
