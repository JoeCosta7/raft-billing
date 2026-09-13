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
	ListTenants() ([]*model.Tenant, error)
	GetTenant(tenantID string) (*model.Tenant, error)
	GetSchedule(tenantID, id string) (*model.Schedule, error)
	GetExecution(tenantID, id string) (*model.Execution, error)
	GetAttempt(tenantID, id string) (*model.Attempt, error)
	ListExecutionsBySchedule(tenantID, scheduleID string) ([]*model.Execution, error)
	ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error)
	ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error)
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
