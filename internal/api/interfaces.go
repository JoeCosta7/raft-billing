package api

import (
	"raft-biling/internal/model"
	"time"
)

// Reader is the read-side surface the API needs from a Raft node. It mirrors
// scheduler.Reader's shape (leader-gated, storage-backed) but is declared
// separately since the API needs a different method set (tenant/execution/
// attempt lookups a client asks for, not the scheduling-loop's own
// ListSchedulesDue).
type Reader interface {
	IsLeader() bool
	LeaderAddr() string
	GetTenant(tenantID string) (*model.Tenant, error)
	GetSchedule(tenantID, id string) (*model.Schedule, error)
	GetExecution(tenantID, id string) (*model.Execution, error)
	GetAttempt(tenantID, id string) (*model.Attempt, error)
	ListTenantsPage(limit int, cursor string) (tenants []*model.Tenant, nextCursor string, err error)
	ListExecutionsBySchedulePage(tenantID, scheduleID string, limit int, cursor string) (executions []*model.Execution, nextCursor string, err error)
	ListExecutionsByStatusPage(tenantID string, status model.ExecutionStatus, limit int, cursor string) (executions []*model.Execution, nextCursor string, err error)
	ListAttemptsByExecutionPage(tenantID, executionID string, limit int, cursor string) (attempts []*model.Attempt, nextCursor string, err error)
}

type Proposer interface {
	Propose(cmdType string, cmd any, timeout time.Duration) (any, error)
}

// Backend is everything a handler needs; *raftnode.RaftNode satisfies it
// structurally, same as it does scheduler.Reader/Proposer.
type Backend interface {
	Reader
	Proposer
}
