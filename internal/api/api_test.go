package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"testing"
	"time"
)

const (
	testAdminKey  = "test-admin-key"
	testTenantKey = "test-tenant-key"
)

type fakeBackend struct {
	isLeader   bool
	leaderAddr string

	proposeFn func(cmdType string, cmd any, timeout time.Duration) (any, error)

	tenants          map[string]*model.Tenant
	schedules        map[string]*model.Schedule
	executions       map[string]*model.Execution
	attempts         map[string]*model.Attempt
	executionsByExec map[string][]*model.Attempt
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		isLeader:         true,
		tenants:          map[string]*model.Tenant{},
		schedules:        map[string]*model.Schedule{},
		executions:       map[string]*model.Execution{},
		attempts:         map[string]*model.Attempt{},
		executionsByExec: map[string][]*model.Attempt{},
	}
}

// seedTenant adds a tenant whose API key is testTenantKey, for tests that
// need a tenant-scoped request to actually authenticate successfully.
func (f *fakeBackend) seedTenant(tenantID string) {
	f.tenants[tenantID] = &model.Tenant{ID: tenantID, APIKeyHash: hashAPIKey(testTenantKey)}
}

func (f *fakeBackend) IsLeader() bool     { return f.isLeader }
func (f *fakeBackend) LeaderAddr() string { return f.leaderAddr }
func (f *fakeBackend) Propose(cmdType string, cmd any, timeout time.Duration) (any, error) {
	return f.proposeFn(cmdType, cmd, timeout)
}

func (f *fakeBackend) ListTenants() ([]*model.Tenant, error) {
	var out []*model.Tenant
	for _, t := range f.tenants {
		out = append(out, t)
	}
	return out, nil
}
func (f *fakeBackend) GetTenant(tenantID string) (*model.Tenant, error) {
	return f.tenants[tenantID], nil
}
func (f *fakeBackend) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	return f.schedules[tenantID+":"+id], nil
}
func (f *fakeBackend) GetExecution(tenantID, id string) (*model.Execution, error) {
	return f.executions[tenantID+":"+id], nil
}
func (f *fakeBackend) GetAttempt(tenantID, id string) (*model.Attempt, error) {
	return f.attempts[tenantID+":"+id], nil
}
func (f *fakeBackend) ListExecutionsBySchedule(tenantID, scheduleID string) ([]*model.Execution, error) {
	var out []*model.Execution
	for _, e := range f.executions {
		if e.TenantID == tenantID && e.ScheduleID == scheduleID {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeBackend) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error) {
	var out []*model.Execution
	for _, e := range f.executions {
		if e.TenantID == tenantID && e.Status == status {
			out = append(out, e)
		}
	}
	return out, nil
}
func (f *fakeBackend) ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error) {
	return f.executionsByExec[tenantID+":"+executionID], nil
}

func newTestAPI(backend *fakeBackend) *API {
	return &API{backend: backend, cfg: &config.Config{AdminKey: testAdminKey}, logger: slog.New(slog.DiscardHandler)}
}

func doRequest(t *testing.T, a *API, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	a.routes().ServeHTTP(rec, req)
	return rec
}

func TestHandleCreateTenant_HappyPath(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		if cmdType != "create_tenant" {
			t.Fatalf("cmdType: got %q, want create_tenant", cmdType)
		}
		tc := cmd.(command.CreateTenantCommand)
		if tc.APIKeyHash == "" {
			t.Error("expected the handler to have generated and set an APIKeyHash before proposing")
		}
		tenant := &model.Tenant{ID: tc.ID, Name: tc.Name, APIKeyHash: tc.APIKeyHash}
		backend.tenants[tc.ID] = tenant
		return tenant, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1", Name: "Acme"}, testAdminKey)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var got struct {
		model.Tenant
		APIKey string `json:"api_key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != "t1" || got.Name != "Acme" {
		t.Errorf("tenant: got %+v", got)
	}
	if got.APIKey == "" {
		t.Error("expected a one-time api_key in the response")
	}
	if got.APIKeyHash != "" {
		t.Error("response leaked APIKeyHash — sanitizeTenant should have cleared it")
	}
}

func TestHandleCreateTenant_MissingAdminToken401(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		t.Fatal("Propose should not be called without valid admin credentials")
		return nil, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestHandleCreateTenant_WrongAdminToken401(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		t.Fatal("Propose should not be called with the wrong admin credentials")
		return nil, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, "not-the-admin-key")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

// TestHandleCreateTenant_TenantKeyIsNotAdmin401 proves a tenant's own API
// key can't be used for the inherently cross-tenant admin endpoints — the
// two credential types are not interchangeable.
func TestHandleCreateTenant_TenantKeyIsNotAdmin401(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		t.Fatal("Propose should not be called using a tenant key for an admin endpoint")
		return nil, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t2"}, testTenantKey)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func TestHandleCreateTenant_NotLeader(t *testing.T) {
	backend := newFakeBackend()
	backend.isLeader = false
	backend.leaderAddr = "127.0.0.1:9999"
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		t.Fatal("Propose should not be called when not leader")
		return nil, nil
	}
	a := newTestAPI(backend)

	// requireLeader runs before requireAdmin, so this must 503 even though
	// no credentials are presented at all — matching every other handler,
	// which all check leadership first.
	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Error == "" {
		t.Error("expected a non-empty error message")
	}
}

func TestHandleCreateTenant_ValidationErrorMapsTo400(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		return &command.CommandError{Kind: command.KindValidation, Field: "id", Message: "id is missing"}, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{}, testAdminKey)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestHandleCreateTenant_ConflictErrorMapsTo409(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		return &command.CommandError{Kind: command.KindConflict, Message: "already exists"}, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, testAdminKey)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestHandleCreateTenant_TransportErrorMapsTo503(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		return nil, errServiceDown
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, testAdminKey)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleCreateSchedule_TenantIDComesFromPath(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	var gotTenantID string
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		sc := cmd.(command.CreateScheduleCommand)
		gotTenantID = sc.TenantID
		return &model.Schedule{ID: sc.ID, TenantID: sc.TenantID}, nil
	}
	a := newTestAPI(backend)

	// Body deliberately omits tenant_id (or sets a wrong one) — the path
	// value must win, since a client shouldn't be able to write into a
	// tenant scope other than the one in the URL.
	body := map[string]any{"id": "s1", "tenant_id": "wrong-tenant"}
	rec := doRequest(t, a, http.MethodPost, "/tenants/t1/schedules", body, testTenantKey)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if gotTenantID != "t1" {
		t.Errorf("tenant_id used for Propose: got %q, want %q (path must win over body)", gotTenantID, "t1")
	}
}

// TestHandleCreateSchedule_OtherTenantsKeyRejected401 is the core tenant-
// isolation guarantee: tenant A's key must never authorize an action on
// tenant B's resources, even though B is a real, existing tenant.
func TestHandleCreateSchedule_OtherTenantsKeyRejected401(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.tenants["t2"] = &model.Tenant{ID: "t2", APIKeyHash: hashAPIKey("t2-own-key")}
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		t.Fatal("Propose should not be called across tenants")
		return nil, nil
	}
	a := newTestAPI(backend)

	// testTenantKey belongs to t1, not t2.
	rec := doRequest(t, a, http.MethodPost, "/tenants/t2/schedules", map[string]any{"id": "s1"}, testTenantKey)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

// TestHandleCreateSchedule_AdminKeyWorksForAnyTenant proves admin bypasses
// per-tenant scoping, unlike a tenant's own key.
func TestHandleCreateSchedule_AdminKeyWorksForAnyTenant(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		sc := cmd.(command.CreateScheduleCommand)
		return &model.Schedule{ID: sc.ID, TenantID: sc.TenantID}, nil
	}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodPost, "/tenants/t1/schedules", map[string]any{"id": "s1"}, testAdminKey)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func TestHandleGetTenant_NotFound(t *testing.T) {
	backend := newFakeBackend()
	a := newTestAPI(backend)

	// Admin key: a per-tenant key can't even be constructed for a tenant
	// that doesn't exist, so this specifically exercises the handler's own
	// not-found logic rather than the auth layer's rejection.
	rec := doRequest(t, a, http.MethodGet, "/tenants/does-not-exist", nil, testAdminKey)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleGetTenant_HappyPath(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.tenants["t1"].Name = "Acme"
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testTenantKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	var got model.Tenant
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != "t1" {
		t.Errorf("tenant: got %+v", got)
	}
	if got.APIKeyHash != "" {
		t.Error("response leaked APIKeyHash — sanitizeTenant should have cleared it")
	}
}

func TestHandleGetTenant_NotLeader(t *testing.T) {
	backend := newFakeBackend()
	backend.isLeader = false
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, "")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleGetTenant_MissingToken401(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestHandleListExecutionsByStatus_MissingStatusIs400(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants/t1/executions", nil, testTenantKey)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleListExecutionsByStatus_HappyPath(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.executions["t1:e1"] = &model.Execution{ID: "e1", TenantID: "t1", Status: model.ExecutionStatusInFlight}
	backend.executions["t1:e2"] = &model.Execution{ID: "e2", TenantID: "t1", Status: model.ExecutionStatusSucceeded}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants/t1/executions?status=in_flight", nil, testTenantKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var got []*model.Execution
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != "e1" {
		t.Errorf("executions: got %+v, want just e1", got)
	}
}

func TestHandlePauseCancelResumeSchedule_UsePathIDs(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	var lastCmdType string
	var lastTenantID, lastScheduleID string
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		lastCmdType = cmdType
		switch c := cmd.(type) {
		case command.PauseScheduleCommand:
			lastTenantID, lastScheduleID = c.TenantID, c.ID
		case command.CancelScheduleCommand:
			lastTenantID, lastScheduleID = c.TenantID, c.ID
		case command.ResumeScheduleCommand:
			lastTenantID, lastScheduleID = c.TenantID, c.ID
		}
		return &model.Schedule{ID: lastScheduleID, TenantID: lastTenantID}, nil
	}
	a := newTestAPI(backend)

	cases := []struct {
		path    string
		wantCmd string
	}{
		{"/tenants/t1/schedules/s1/pause", "pause_schedule"},
		{"/tenants/t1/schedules/s1/cancel", "cancel_schedule"},
		{"/tenants/t1/schedules/s1/resume", "resume_schedule"},
	}
	for _, tc := range cases {
		rec := doRequest(t, a, http.MethodPost, tc.path, nil, testTenantKey)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status: got %d, want %d, body: %s", tc.path, rec.Code, http.StatusOK, rec.Body.String())
		}
		if lastCmdType != tc.wantCmd {
			t.Errorf("%s: cmdType: got %q, want %q", tc.path, lastCmdType, tc.wantCmd)
		}
		if lastTenantID != "t1" || lastScheduleID != "s1" {
			t.Errorf("%s: ids: got tenant=%q schedule=%q", tc.path, lastTenantID, lastScheduleID)
		}
	}
}

var errServiceDown = &testError{"service down"}

type testError struct{ msg string }

func (e *testError) Error() string { return e.msg }
