package scheduler

import (
	"context"
	"log/slog"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
)

type Scheduler struct {
	rn     *raftnode.RaftNode
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

func New(rn *raftnode.RaftNode, sm *statemachine.StateMachine) *Scheduler {
	return &Scheduler{rn: rn, logger: slog.Default()}
}

func (scheduler *Scheduler) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	scheduler.cancel = cancel
	scheduler.done = make(chan struct{})

	newWorker := func() Runner {
		return NewWorker(scheduler.rn, scheduler.rn, scheduler.logger)
	}
	supervisor := NewSupervisor(newWorker, scheduler.rn.LeadershipCh(), scheduler.logger)

	go func() {
		defer close(scheduler.done)
		if err := supervisor.Run(runCtx); err != nil {
			scheduler.logger.Error("supervisor exited with error", "error", err)
		}
	}()
	return nil
}

func (scheduler *Scheduler) Shutdown(ctx context.Context) error {
	if scheduler.cancel == nil {
		return nil
	}
	scheduler.cancel()
	select {
	case <-scheduler.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
