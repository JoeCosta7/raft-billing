package statemachine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"raft-biling/internal/command"
	"raft-biling/internal/config"
	"raft-biling/internal/storage"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

type StateMachine struct {
	storage storage.Storage
	db      *bolt.DB
	dataDir string
}

type FSMSnapshot struct {
	tx *bolt.Tx
}

func (sm *StateMachine) Apply(log *raft.Log) any {
	var envelope command.LogEntry
	if err := json.Unmarshal(log.Data, &envelope); err != nil {
		panic(err)
	}
	var result any
	err := sm.storage.Update(func(tx storage.Tx) error {
		res, herr := command.Dispatch(tx, envelope.Type, envelope.Payload, envelope.ProposedAt)
		result = res
		return herr
	})
	var cmdErr *command.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr
	}
	if err != nil {
		panic(err)
	}
	return result
}

func (sm *StateMachine) Storage() storage.Storage {
	return sm.storage
}

func (sm *StateMachine) Snapshot() (raft.FSMSnapshot, error) {
	tx, err := sm.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &FSMSnapshot{tx: tx}, nil
}

func (sm *StateMachine) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	err := sm.db.Close()
	if err != nil {
		return err
	}
	path := filepath.Join(sm.dataDir, "state.db")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, rc)
	if err != nil {
		return err
	}
	file.Close()
	newDB, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return err
	}
	sm.db = newDB

	return nil

}

func (s *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := s.tx.WriteTo(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *FSMSnapshot) Release() {
	s.tx.Rollback()
}

func New(cfg *config.Config) (*StateMachine, error) {
	store, err := storage.New(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	return &StateMachine{storage: store, db: store.DB(), dataDir: cfg.DataDir}, nil
}

func (statemachine *StateMachine) Start(ctx context.Context) error { return nil }

func (statemachine *StateMachine) Shutdown(ctx context.Context) error {
	return statemachine.storage.Close()
}
