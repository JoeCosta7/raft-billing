package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"sort"
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

func paginate[T any](items []T, limit int, cursor string, idOf func(T) string) ([]T, string) {
	sort.Slice(items, func(i, j int) bool { return idOf(items[i]) < idOf(items[j]) })
	if limit <= 0 {
		return nil, ""
	}
	start := 0
	if cursor != "" {
		start = sort.Search(len(items), func(i int) bool { return idOf(items[i]) > cursor })
	}
	if start >= len(items) {
		return nil, ""
	}
	end := start + limit
	if end >= len(items) {
		return items[start:], ""
	}
	return items[start:end], idOf(items[end-1])
}

func (f *fakeBackend) ListTenantsPage(limit int, cursor string) ([]*model.Tenant, string, error) {
	var out []*model.Tenant
	for _, t := range f.tenants {
		out = append(out, t)
	}
	page, nextCursor := paginate(out, limit, cursor, func(t *model.Tenant) string { return t.ID })
	return page, nextCursor, nil
}
func (f *fakeBackend) ListExecutionsBySchedulePage(tenantID, scheduleID string, limit int, cursor string) ([]*model.Execution, string, error) {
	var out []*model.Execution
	for _, e := range f.executions {
		if e.TenantID == tenantID && e.ScheduleID == scheduleID {
			out = append(out, e)
		}
	}
	page, nextCursor := paginate(out, limit, cursor, func(e *model.Execution) string { return e.ID })
	return page, nextCursor, nil
}
func (f *fakeBackend) ListExecutionsByStatusPage(tenantID string, status model.ExecutionStatus, limit int, cursor string) ([]*model.Execution, string, error) {
	var out []*model.Execution
	for _, e := range f.executions {
		if e.TenantID == tenantID && e.Status == status {
			out = append(out, e)
		}
	}
	page, nextCursor := paginate(out, limit, cursor, func(e *model.Execution) string { return e.ID })
	return page, nextCursor, nil
}
func (f *fakeBackend) ListAttemptsByExecutionPage(tenantID, executionID string, limit int, cursor string) ([]*model.Attempt, string, error) {
	out := f.executionsByExec[tenantID+":"+executionID]
	page, nextCursor := paginate(out, limit, cursor, func(a *model.Attempt) string { return a.ID })
	return page, nextCursor, nil
}

func newTestAPI(backend *fakeBackend) *API {
	return &API{
		backend: backend,
		cfg:     &config.Config{AdminKey: testAdminKey},
		logger:  slog.New(slog.DiscardHandler),
		limiter: newRateLimiter(defaultRatePerSecond, defaultBurst),
	}
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

func TestHandleListTenants_HappyPath(t *testing.T) {
	backend := newFakeBackend()
	backend.tenants["t1"] = &model.Tenant{ID: "t1", Name: "Acme", APIKeyHash: "should-not-leak"}
	backend.tenants["t2"] = &model.Tenant{ID: "t2", Name: "Globex", APIKeyHash: "should-not-leak"}
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants", nil, testAdminKey)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var got pageResponse[*model.Tenant]
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) != 2 {
		t.Fatalf("tenants: got %d, want 2", len(got.Items))
	}
	for _, tenant := range got.Items {
		if tenant.APIKeyHash != "" {
			t.Error("response leaked APIKeyHash — sanitizeTenants should have cleared it")
		}
	}
}

func TestHandleListTenants_TenantKeyRejected401(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	a := newTestAPI(backend)

	rec := doRequest(t, a, http.MethodGet, "/tenants", nil, testTenantKey)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusUnauthorized)
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
	var got pageResponse[*model.Execution]
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Items) != 1 || got.Items[0].ID != "e1" {
		t.Errorf("executions: got %+v, want just e1", got.Items)
	}
	if got.NextCursor != "" {
		t.Errorf("next_cursor: got %q, want empty (only one matching item)", got.NextCursor)
	}
}

func TestHandleListExecutionsByStatus_LimitValidation(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	a := newTestAPI(backend)

	for _, limit := range []string{"0", "-1", "not-a-number", "201"} {
		rec := doRequest(t, a, http.MethodGet, "/tenants/t1/executions?status=in_flight&limit="+limit, nil, testTenantKey)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("limit=%s: status: got %d, want %d", limit, rec.Code, http.StatusBadRequest)
		}
	}
}

func TestHandleListExecutionsByStatus_PaginatesWithCursor(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	for i := range 5 {
		id := fmt.Sprintf("e%d", i)
		backend.executions["t1:"+id] = &model.Execution{ID: id, TenantID: "t1", Status: model.ExecutionStatusInFlight}
	}
	a := newTestAPI(backend)

	var allIDs []string
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		rec := doRequest(t, a, http.MethodGet, "/tenants/t1/executions?status=in_flight&limit=2&cursor="+cursor, nil, testTenantKey)
		if rec.Code != http.StatusOK {
			t.Fatalf("status: got %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
		}
		var got pageResponse[*model.Execution]
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(got.Items) > 2 {
			t.Fatalf("page size: got %d, want <= 2", len(got.Items))
		}
		for _, e := range got.Items {
			allIDs = append(allIDs, e.ID)
		}
		if got.NextCursor == "" {
			break
		}
		cursor = got.NextCursor
	}

	want := []string{"e0", "e1", "e2", "e3", "e4"}
	if len(allIDs) != len(want) {
		t.Fatalf("collected IDs: got %v, want %v", allIDs, want)
	}
	for i, id := range want {
		if allIDs[i] != id {
			t.Errorf("position %d: got %q, want %q", i, allIDs[i], id)
		}
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

func TestRequireAdmin_RateLimitExceeded429(t *testing.T) {
	backend := newFakeBackend()
	backend.proposeFn = func(cmdType string, cmd any, timeout time.Duration) (any, error) {
		tc := cmd.(command.CreateTenantCommand)
		tenant := &model.Tenant{ID: tc.ID}
		backend.tenants[tc.ID] = tenant
		return tenant, nil
	}
	a := newTestAPI(backend)
	a.limiter = newRateLimiter(1, 1)

	first := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t1"}, testAdminKey)
	if first.Code != http.StatusCreated {
		t.Fatalf("first request: status: got %d, want %d, body: %s", first.Code, http.StatusCreated, first.Body.String())
	}

	second := doRequest(t, a, http.MethodPost, "/tenants", command.CreateTenantCommand{ID: "t2"}, testAdminKey)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: status: got %d, want %d, body: %s", second.Code, http.StatusTooManyRequests, second.Body.String())
	}
	if second.Header().Get("Retry-After") == "" {
		t.Error("expected a Retry-After header on the 429 response")
	}
}

func TestRequireTenantAccess_RateLimitExceeded429(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	a := newTestAPI(backend)
	a.limiter = newRateLimiter(1, 1)

	first := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testTenantKey)
	if first.Code != http.StatusOK {
		t.Fatalf("first request: status: got %d, want %d", first.Code, http.StatusOK)
	}

	second := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testTenantKey)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: status: got %d, want %d, body: %s", second.Code, http.StatusTooManyRequests, second.Body.String())
	}
}

func TestRequireTenantAccess_RateLimitsAreIndependentPerTenant(t *testing.T) {
	backend := newFakeBackend()
	backend.seedTenant("t1")
	backend.tenants["t2"] = &model.Tenant{ID: "t2", APIKeyHash: hashAPIKey("t2-key")}
	a := newTestAPI(backend)
	a.limiter = newRateLimiter(1, 1)

	// Exhaust t1's budget.
	doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testTenantKey)
	exhausted := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testTenantKey)
	if exhausted.Code != http.StatusTooManyRequests {
		t.Fatalf("t1 second request: status: got %d, want %d", exhausted.Code, http.StatusTooManyRequests)
	}

	// t2 has never made a request, so its independent budget is untouched.
	stillOK := doRequest(t, a, http.MethodGet, "/tenants/t2", nil, "t2-key")
	if stillOK.Code != http.StatusOK {
		t.Fatalf("t2 first request: status: got %d, want %d, body: %s", stillOK.Code, http.StatusOK, stillOK.Body.String())
	}
}

// TestRequireTenantAccess_AdminSharesAdminBucketAcrossTenants proves the
// rate-limit identity is the credential ("admin"), not the resource path --
// an admin key spending its budget against one tenant's path leaves it
// exhausted against a completely different tenant's path too.
func TestRequireTenantAccess_AdminSharesAdminBucketAcrossTenants(t *testing.T) {
	backend := newFakeBackend()
	backend.tenants["t1"] = &model.Tenant{ID: "t1"}
	backend.tenants["t2"] = &model.Tenant{ID: "t2"}
	a := newTestAPI(backend)
	a.limiter = newRateLimiter(1, 1)

	first := doRequest(t, a, http.MethodGet, "/tenants/t1", nil, testAdminKey)
	if first.Code != http.StatusOK {
		t.Fatalf("admin request against t1: status: got %d, want %d", first.Code, http.StatusOK)
	}

	second := doRequest(t, a, http.MethodGet, "/tenants/t2", nil, testAdminKey)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("admin request against t2: status: got %d, want %d (admin bucket should already be exhausted)", second.Code, http.StatusTooManyRequests)
	}
}
