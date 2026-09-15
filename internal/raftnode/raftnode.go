package raftnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/model"
	"raft-biling/internal/statemachine"
	"raft-biling/internal/storage"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// barrierTimeout bounds the one-time-per-term wait in ensureCaughtUpAsLeader.
const barrierTimeout = 5 * time.Second

var ErrNotCaughtUp = errors.New("leader elected but not yet caught up")

type RaftNode struct {
	raft        *raft.Raft
	storage     storage.Storage
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	transport   *raft.NetworkTransport
	id          raft.ServerID
	notifyCh    chan bool
	readyMu     sync.Mutex
	readyTerm   uint64
}

func (rn *RaftNode) TransferLeadership() error {
	return rn.raft.LeadershipTransfer().Error()
}

// ensureCaughtUpAsLeader blocks until this node's local FSM has applied
// every entry committed under any previous leader term
func (rn *RaftNode) ensureCaughtUpAsLeader() error {
	term := rn.raft.CurrentTerm()
	rn.readyMu.Lock()
	defer rn.readyMu.Unlock()
	if rn.readyTerm == term {
		return nil
	}
	if err := rn.raft.Barrier(barrierTimeout).Error(); err != nil {
		return fmt.Errorf("%w: %v", ErrNotCaughtUp, err)
	}
	rn.readyTerm = term
	return nil
}

func (rn *RaftNode) LeadershipCh() <-chan bool {
	return rn.notifyCh
}

func New(cfg *config.Config, st storage.Storage, fsm *statemachine.StateMachine) (*RaftNode, error) {
	localID := raft.ServerID(cfg.NodeID)
	config := raft.DefaultConfig()
	config.LocalID = localID
	notifyCh := make(chan bool, 1)
	config.NotifyCh = notifyCh
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("Could not create logStore : %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("Could not create stableStore : %w", err)
	}
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create snapshotStore : %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create transport : %w", err)
	}
	newRaft, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("Could not create raft : %w", err)
	}
	var servers []raft.Server
	for nodeID, addr := range cfg.Peers {
		server := raft.Server{
			ID:       raft.ServerID(nodeID),
			Address:  raft.ServerAddress(addr),
			Suffrage: raft.Voter,
		}
		servers = append(servers, server)
	}
	configuration := raft.Configuration{
		Servers: servers,
	}
	var future raft.Future
	if cfg.Bootstrap {
		future = newRaft.BootstrapCluster(configuration)
		if future.Error() != nil {
			return nil, fmt.Errorf("Could not bootstrap the cluster %w", future.Error())
		}
	}
	return &RaftNode{
		raft:        newRaft,
		storage:     st,
		logStore:    logStore,
		stableStore: stableStore,
		transport:   transport,
		id:          localID,
		notifyCh:    notifyCh,
	}, nil

}

func (rn *RaftNode) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to GetSchedule on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var schedule *model.Schedule
	err := rn.storage.View(func(tx storage.Tx) error {
		s, err := tx.GetSchedule(tenantID, id)
		if err != nil {
			return err
		}
		schedule = s
		return nil
	})
	if err != nil {
		return nil, err
	}
	return schedule, nil
}

func (rn *RaftNode) ListSchedulesDue(tenantID string) ([]*model.Schedule, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to ListSchedulesDue on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var schedules []*model.Schedule
	err := rn.storage.View(func(tx storage.Tx) error {
		return tx.ListSchedulesDue(tenantID, time.Now(), func(s *model.Schedule) error {
			schedules = append(schedules, s)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return schedules, nil
}

func (rn *RaftNode) GetTenant(tenantID string) (*model.Tenant, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("not leader; current leader is %s", rn.raft.Leader())
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var tenant *model.Tenant
	err := rn.storage.View(func(tx storage.Tx) error {
		te, err := tx.GetTenant(tenantID)
		if err != nil {
			return err
		}
		tenant = te
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func (rn *RaftNode) ListTenants() ([]*model.Tenant, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to ListTenants on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var tenants []*model.Tenant
	err := rn.storage.View(func(tx storage.Tx) error {
		ts, err := tx.ListTenants()
		if err != nil {
			return err
		}
		tenants = ts
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenants, nil
}

func (rn *RaftNode) GetExecution(tenantID, id string) (*model.Execution, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to GetExecution on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var execution *model.Execution
	err := rn.storage.View(func(tx storage.Tx) error {
		ex, err := tx.GetExecution(tenantID, id)
		if err != nil {
			return err
		}
		execution = ex
		return nil
	})
	if err != nil {
		return nil, err
	}
	return execution, nil
}

func (rn *RaftNode) GetAttempt(tenantID, id string) (*model.Attempt, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to GetAttempt on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var attempt *model.Attempt
	err := rn.storage.View(func(tx storage.Tx) error {
		at, err := tx.GetAttempt(tenantID, id)
		if err != nil {
			return err
		}
		attempt = at
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attempt, nil
}

func (rn *RaftNode) ListExecutionsBySchedule(tenantID, scheduleID string) ([]*model.Execution, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to ListExecutionsBySchedule on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var executions []*model.Execution
	err := rn.storage.View(func(tx storage.Tx) error {
		err := tx.ListExecutionsBySchedule(tenantID, scheduleID, func(ex *model.Execution) error {
			executions = append(executions, ex)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return executions, nil
}

func (rn *RaftNode) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to ListExecutionsByStatus on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var executions []*model.Execution
	err := rn.storage.View(func(tx storage.Tx) error {
		err := tx.ListExecutionsByStatus(tenantID, status, func(ex *model.Execution) error {
			executions = append(executions, ex)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return executions, nil
}

func (rn *RaftNode) ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error) {
	if rn.raft.State() != raft.Leader {
		return nil, fmt.Errorf("failed to ListAttemptsByExecution on node %v", string(rn.raft.Leader()))
	}
	if err := rn.ensureCaughtUpAsLeader(); err != nil {
		return nil, err
	}
	var attempts []*model.Attempt
	err := rn.storage.View(func(tx storage.Tx) error {
		err := tx.ListAttemptsByExecution(tenantID, executionID, func(at *model.Attempt) error {
			attempts = append(attempts, at)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func (rn *RaftNode) Propose(cmdType string, cmd any, timeout time.Duration) (any, error) {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("Error while marshalling command to json: %w", err)
	}
	logEntry := command.LogEntry{
		Type:       cmdType,
		ProposedAt: time.Now(),
		Payload:    json.RawMessage(cmdBytes),
	}
	data, err := json.Marshal(logEntry)
	if err != nil {
		return nil, fmt.Errorf("Error while marshalling to json %w", err)
	}
	future := rn.raft.Apply(data, timeout)
	if future.Error() != nil {
		return nil, fmt.Errorf("Could not apply %w", future.Error())
	}
	return future.Response(), nil
}

func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

func (rn *RaftNode) ID() string {
	return string(rn.id)
}

func (rn *RaftNode) LeaderAddr() string {
	return string(rn.raft.Leader())
}

func (raftnode *RaftNode) Start(ctx context.Context) error { return nil }

func (rn *RaftNode) Shutdown(ctx context.Context) error {
	var firstErr error
	if err := rn.raft.Shutdown().Error(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := rn.logStore.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := rn.stableStore.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := rn.transport.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
