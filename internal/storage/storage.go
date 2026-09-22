package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"raft-biling/internal/model"
	"time"

	bolt "go.etcd.io/bbolt"
)

// persistent backend
type Storage interface {
	Update(func(tx Tx) error) error
	View(func(tx Tx) error) error
	Close() error
}

type Tx interface {
	GetSchedule(tenantID, id string) (*model.Schedule, error)
	PutSchedule(s *model.Schedule) error

	GetExecution(tenantID, id string) (*model.Execution, error)
	PutExecution(ex *model.Execution) error

	GetAttempt(tenantID, id string) (*model.Attempt, error)
	PutAttempt(a *model.Attempt) error

	PutTenant(t *model.Tenant) error
	GetTenant(id string) (*model.Tenant, error)
	ListTenants() ([]*model.Tenant, error)

	ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error
	ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error
	ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error
	ListSchedulesDue(tenantID string, now time.Time, fn func(*model.Schedule) error) error

	// The Page-suffixed methods below are the paginated counterparts of the
	// unpaginated ones above, used only by the HTTP API. The unpaginated
	// ones stay as-is because internal/scheduler's recovery/tick loop needs
	// the complete result set every call -- paginating those would silently
	// make crash recovery miss orphaned executions past page one.
	ListTenantsPage(limit int, cursor string) (tenants []*model.Tenant, nextCursor string, err error)
	ListExecutionsBySchedulePage(tenantID, scheduleID string, limit int, cursor string) (executions []*model.Execution, nextCursor string, err error)
	ListExecutionsByStatusPage(tenantID string, status model.ExecutionStatus, limit int, cursor string) (executions []*model.Execution, nextCursor string, err error)
	ListAttemptsByExecutionPage(tenantID, executionID string, limit int, cursor string) (attempts []*model.Attempt, nextCursor string, err error)
}

const (
	bucketSchedules            = "schedules"
	bucketExecutions           = "executions"
	bucketAttempts             = "attempts"
	bucketTenants              = "tenants"
	bucketExecutionsBySchedule = "executions_by_schedule"
	bucketExecutionsByStatus   = "executions_by_status"
	bucketAttemptsByExecution  = "attempts_by_execution"
	bucketSchedulesByStatus    = "schedules_by_status"
)

// bbolt implementation
type BoltStorage struct {
	db *bolt.DB
}

func (s *BoltStorage) Update(fn func(tx Tx) error) error {
	return s.db.Update(func(btx *bolt.Tx) error {
		return fn(&boltTx{tx: btx})
	})
}

func (s *BoltStorage) View(fn func(tx Tx) error) error {
	return s.db.View(func(btx *bolt.Tx) error {
		return fn(&boltTx{tx: btx})
	})
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}

func (s *BoltStorage) DB() *bolt.DB {
	return s.db
}

// FromDB wraps an already-open bbolt database.
func FromDB(db *bolt.DB) *BoltStorage {
	return &BoltStorage{db: db}
}

func New(dataDir string) (*BoltStorage, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dataDir, "scheduler.db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			[]byte(bucketSchedules),
			[]byte(bucketExecutions),
			[]byte(bucketAttempts),
			[]byte(bucketTenants),
			[]byte(bucketExecutionsBySchedule),
			[]byte(bucketExecutionsByStatus),
			[]byte(bucketAttemptsByExecution),
			[]byte(bucketSchedulesByStatus),
		}
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStorage{db: db}, nil

}

type boltTx struct {
	tx *bolt.Tx
}

func scheduleKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func executionKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func attemptKey(tenantID, id string) []byte {
	return []byte(tenantID + ":" + id)
}

func executionByScheduleKey(tenantID, scheduleID, id string) []byte {
	return []byte(tenantID + ":" + scheduleID + ":" + id)
}

func executionByStatusKey(tenantID, status, id string) []byte {
	return []byte(tenantID + ":" + status + ":" + id)
}

func attemptsByExecutionKey(tenantID, executionID, id string) []byte {
	return []byte(tenantID + ":" + executionID + ":" + id)
}

func scheduleByStatusKey(tenantID, status, id string) []byte {
	return []byte(tenantID + ":" + status + ":" + id)
}

func (t *boltTx) PutSchedule(s *model.Schedule) error {
	oldRow, err := t.GetSchedule(s.TenantID, s.ID)
	if err != nil {
		return fmt.Errorf("Could not find the schedule %w", err)
	}
	if oldRow != nil {
		err := t.tx.Bucket([]byte(bucketSchedulesByStatus)).Delete(scheduleByStatusKey(oldRow.TenantID, string(oldRow.Status), oldRow.ID))
		if err != nil {
			return fmt.Errorf("Delete was not successful %w", err)
		}
	}
	jsonData, err := json.Marshal(s)
	if err != nil {
		return err
	}
	key := scheduleKey(s.TenantID, s.ID)
	statusKey := scheduleByStatusKey(s.TenantID, string(s.Status), s.ID)
	if err := t.tx.Bucket([]byte(bucketSchedules)).Put(key, jsonData); err != nil {
		return fmt.Errorf("Put to Schedules Bucket was unsuccessful %w", err)
	}
	if err := t.tx.Bucket([]byte(bucketSchedulesByStatus)).Put(statusKey, nil); err != nil {
		return fmt.Errorf("Put to Schedules By Status Bucket was unsuccessful %w", err)
	}
	return nil
}

func (t *boltTx) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	key := scheduleKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketSchedules)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var s model.Schedule
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (t *boltTx) PutExecution(ex *model.Execution) error {
	oldRow, err := t.GetExecution(ex.TenantID, ex.ID)
	if err != nil {
		return fmt.Errorf("Could not find the execution %w", err)
	}
	if oldRow != nil {
		err := t.tx.Bucket([]byte(bucketExecutionsByStatus)).Delete(executionByStatusKey(oldRow.TenantID, string(oldRow.Status), oldRow.ID))
		if err != nil {
			return fmt.Errorf("Delete was not successful %w", err)
		}
	}
	jsonData, err := json.Marshal(ex)
	if err != nil {
		return err
	}
	exKey := executionKey(ex.TenantID, ex.ID)
	exByStatusKey := executionByStatusKey(ex.TenantID, string(ex.Status), ex.ID)
	exBySchedKey := executionByScheduleKey(ex.TenantID, ex.ScheduleID, ex.ID)
	err = t.tx.Bucket([]byte(bucketExecutions)).Put(exKey, jsonData)
	if err != nil {
		return fmt.Errorf("Put to Executions Bucket was unsuccessful %w", err)
	}
	err = t.tx.Bucket([]byte(bucketExecutionsByStatus)).Put(exByStatusKey, nil)
	if err != nil {
		return fmt.Errorf("Put to Executions By Status was Bucket was unsuccessful %w", err)
	}
	err = t.tx.Bucket([]byte(bucketExecutionsBySchedule)).Put(exBySchedKey, nil)
	if err != nil {
		return fmt.Errorf("Put to Executions By Schedule Bucket was unsuccessful %w", err)
	}
	return nil
}

func (t *boltTx) GetExecution(tenantID, id string) (*model.Execution, error) {
	key := executionKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketExecutions)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var ex model.Execution
	if err := json.Unmarshal(raw, &ex); err != nil {
		return nil, err
	}
	return &ex, nil
}

func (t *boltTx) PutAttempt(at *model.Attempt) error {
	jsonData, err := json.Marshal(at)
	if err != nil {
		return err
	}
	key := attemptKey(at.TenantID, at.ID)
	attemptByExecKey := attemptsByExecutionKey(at.TenantID, at.ExecutionID, at.ID)
	err = t.tx.Bucket([]byte(bucketAttempts)).Put(key, jsonData)
	if err != nil {
		return fmt.Errorf("Put to Attempts Bucket was unsuccessful %w", err)
	}
	err = t.tx.Bucket([]byte(bucketAttemptsByExecution)).Put(attemptByExecKey, nil)
	if err != nil {
		return fmt.Errorf("Put to Attempts By Execution Bucket was unsuccessful %w", err)
	}
	return nil
}

func (t *boltTx) GetAttempt(tenantID, id string) (*model.Attempt, error) {
	key := attemptKey(tenantID, id)
	raw := t.tx.Bucket([]byte(bucketAttempts)).Get(key)
	if raw == nil {
		return nil, nil
	}
	var at model.Attempt
	if err := json.Unmarshal(raw, &at); err != nil {
		return nil, err
	}
	return &at, nil
}

func (t *boltTx) PutTenant(tenant *model.Tenant) error {
	jsonData, err := json.Marshal(tenant)
	if err != nil {
		return err
	}
	return t.tx.Bucket([]byte(bucketTenants)).Put([]byte(tenant.ID), jsonData)
}

func (t *boltTx) GetTenant(id string) (*model.Tenant, error) {
	raw := t.tx.Bucket([]byte(bucketTenants)).Get([]byte(id))
	if raw == nil {
		return nil, nil
	}
	var tenant model.Tenant
	if err := json.Unmarshal(raw, &tenant); err != nil {
		return nil, err
	}
	return &tenant, nil
}

func (t *boltTx) ListTenants() ([]*model.Tenant, error) {
	b := t.tx.Bucket([]byte(bucketTenants))
	var tenants []*model.Tenant
	err := b.ForEach(func(k, raw []byte) error {
		var tenant model.Tenant
		if err := json.Unmarshal(raw, &tenant); err != nil {
			return err
		}
		tenants = append(tenants, &tenant)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenants, nil
}

// ListTenantsPage walks bucketTenants (keyed directly by tenant ID, no
// prefix) in cursor order, which is the same byte-sorted order ListTenants'
// ForEach already produces -- pagination doesn't change ordering, just
// exposes it explicitly. See the "peek +1" note on ListExecutionsByStatusPage
// for how nextCursor is determined.
func (t *boltTx) ListTenantsPage(limit int, cursor string) ([]*model.Tenant, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	b := t.tx.Bucket([]byte(bucketTenants))
	c := b.Cursor()
	var k, v []byte
	if cursor == "" {
		k, v = c.First()
	} else {
		k, v = c.Seek([]byte(cursor))
		if k != nil && string(k) == cursor {
			k, v = c.Next()
		}
	}
	var tenants []*model.Tenant
	var nextCursor string
	for ; k != nil; k, v = c.Next() {
		if len(tenants) == limit {
			nextCursor = tenants[len(tenants)-1].ID
			break
		}
		var tenant model.Tenant
		if err := json.Unmarshal(v, &tenant); err != nil {
			return nil, "", err
		}
		tenants = append(tenants, &tenant)
	}
	return tenants, nextCursor, nil
}

func (t *boltTx) ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error {
	b := t.tx.Bucket([]byte(bucketExecutionsBySchedule))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + scheduleID + ":")
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		execID := string(bytes.TrimPrefix(k, prefix))
		exec, err := t.GetExecution(tenantID, execID)
		if err != nil {
			return err
		}
		if exec == nil {
			return fmt.Errorf("index entry references missing execution: tenant=%s exec_id=%s", tenantID, execID)
		}
		if err := fn(exec); err != nil {
			return err
		}
	}
	return nil
}

// ListExecutionsBySchedulePage is the paginated counterpart of
// ListExecutionsBySchedule -- see ListExecutionsByStatusPage below for the
// cursor/peek-+1 pattern shared by all three prefix-scan Page methods.
func (t *boltTx) ListExecutionsBySchedulePage(tenantID, scheduleID string, limit int, cursor string) ([]*model.Execution, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	b := t.tx.Bucket([]byte(bucketExecutionsBySchedule))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + scheduleID + ":")
	startKey := prefix
	if cursor != "" {
		startKey = executionByScheduleKey(tenantID, scheduleID, cursor)
	}
	k, _ := c.Seek(startKey)
	if cursor != "" && k != nil && bytes.Equal(k, startKey) {
		k, _ = c.Next()
	}
	var executions []*model.Execution
	var nextCursor string
	for ; k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if len(executions) == limit {
			nextCursor = executions[len(executions)-1].ID
			break
		}
		execID := string(bytes.TrimPrefix(k, prefix))
		exec, err := t.GetExecution(tenantID, execID)
		if err != nil {
			return nil, "", err
		}
		if exec == nil {
			return nil, "", fmt.Errorf("index entry references missing execution: tenant=%s exec_id=%s", tenantID, execID)
		}
		executions = append(executions, exec)
	}
	return executions, nextCursor, nil
}

func (t *boltTx) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error {
	b := t.tx.Bucket([]byte(bucketExecutionsByStatus))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + string(status) + ":")
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		execID := string(bytes.TrimPrefix(k, prefix))
		exec, err := t.GetExecution(tenantID, execID)
		if err != nil {
			return err
		}
		if exec == nil {
			return fmt.Errorf("index entry references missing execution: tenant=%s exec_id=%s", tenantID, execID)
		}
		if err := fn(exec); err != nil {
			return err
		}
	}
	return nil
}

// ListExecutionsByStatusPage is the paginated counterpart of
// ListExecutionsByStatus. Seeks to prefix+cursor instead of the bare prefix
// when resuming (Seek lands on the cursor's own key if it still exists in
// this status's index -- skip it; if it no longer exists because the
// execution's status changed between pages, Seek already lands on the next
// key >= it, which is exactly the right resume point). Collects up to
// limit items; if a (limit+1)th matching key exists, that proves there's a
// next page, so nextCursor is set to the limit-th item's ID without
// consuming the peeked item.
func (t *boltTx) ListExecutionsByStatusPage(tenantID string, status model.ExecutionStatus, limit int, cursor string) ([]*model.Execution, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	b := t.tx.Bucket([]byte(bucketExecutionsByStatus))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + string(status) + ":")
	startKey := prefix
	if cursor != "" {
		startKey = executionByStatusKey(tenantID, string(status), cursor)
	}
	k, _ := c.Seek(startKey)
	if cursor != "" && k != nil && bytes.Equal(k, startKey) {
		k, _ = c.Next()
	}
	var executions []*model.Execution
	var nextCursor string
	for ; k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if len(executions) == limit {
			nextCursor = executions[len(executions)-1].ID
			break
		}
		execID := string(bytes.TrimPrefix(k, prefix))
		exec, err := t.GetExecution(tenantID, execID)
		if err != nil {
			return nil, "", err
		}
		if exec == nil {
			return nil, "", fmt.Errorf("index entry references missing execution: tenant=%s exec_id=%s", tenantID, execID)
		}
		executions = append(executions, exec)
	}
	return executions, nextCursor, nil
}

func (t *boltTx) ListSchedulesDue(tenantID string, now time.Time, fn func(*model.Schedule) error) error {
	b := t.tx.Bucket([]byte(bucketSchedulesByStatus))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + string(model.ScheduleStatusActive) + ":")
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		schedID := string(bytes.TrimPrefix(k, prefix))
		sched, err := t.GetSchedule(tenantID, schedID)
		if err != nil {
			return err
		}
		if sched == nil {
			return fmt.Errorf("index entry references missing schedule: tenant=%s schedule_id=%s", tenantID, schedID)
		}
		if sched.NextRunAt == nil || sched.NextRunAt.After(now) {
			continue
		}
		if err := fn(sched); err != nil {
			return err
		}
	}
	return nil
}

func (t *boltTx) ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error {
	b := t.tx.Bucket([]byte(bucketAttemptsByExecution))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + executionID + ":")
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		attemptID := string(bytes.TrimPrefix(k, prefix))
		attempt, err := t.GetAttempt(tenantID, attemptID)
		if err != nil {
			return err
		}
		if attempt == nil {
			return fmt.Errorf("index entry references missing attempt: tenant=%s attempt_id=%s", tenantID, attemptID)
		}
		if err := fn(attempt); err != nil {
			return err
		}
	}
	return nil
}

// ListAttemptsByExecutionPage is the paginated counterpart of
// ListAttemptsByExecution -- see ListExecutionsByStatusPage for the
// cursor/peek-+1 pattern shared by all three prefix-scan Page methods.
func (t *boltTx) ListAttemptsByExecutionPage(tenantID, executionID string, limit int, cursor string) ([]*model.Attempt, string, error) {
	if limit <= 0 {
		return nil, "", nil
	}
	b := t.tx.Bucket([]byte(bucketAttemptsByExecution))
	c := b.Cursor()
	prefix := []byte(tenantID + ":" + executionID + ":")
	startKey := prefix
	if cursor != "" {
		startKey = attemptsByExecutionKey(tenantID, executionID, cursor)
	}
	k, _ := c.Seek(startKey)
	if cursor != "" && k != nil && bytes.Equal(k, startKey) {
		k, _ = c.Next()
	}
	var attempts []*model.Attempt
	var nextCursor string
	for ; k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		if len(attempts) == limit {
			nextCursor = attempts[len(attempts)-1].ID
			break
		}
		attemptID := string(bytes.TrimPrefix(k, prefix))
		attempt, err := t.GetAttempt(tenantID, attemptID)
		if err != nil {
			return nil, "", err
		}
		if attempt == nil {
			return nil, "", fmt.Errorf("index entry references missing attempt: tenant=%s attempt_id=%s", tenantID, attemptID)
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nextCursor, nil
}

//User facing
//Create Schedule, check whether a schedule with the tenant_id, & id already exists, read a schedule by tenant_id, id and write a schedule
//Update Schedule, check whether the schedule acutally exists, but get covers that and then update it, which put covers
//Pause Schedule, get to check if schedule exists, then use Put to set status to paused
//Resume Schedule, get to check if schedule exists, then use Put to set status to active
//Cancel Schedule, get to check if schedule exists, then use Put to set status to canceled
//Internal Commands
//Claim Exectuion, check whether exectuion already exists, create if not already existing, set owner
//Adopt Execution, use put to set new owner and update claimed_at
//RecordAttempt appends Attempt row, calls put execution to updates parent's attempt count and last_attempt_id. Maybe need another function for proposer verification
//CompleteExecution, use put to update Execution & clear owner node id. Also put to schedule to advance next_run_at.
//FailExecutionTimeout, use put to update execution failed with timeout reasoning
