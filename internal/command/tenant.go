package command

import (
	"fmt"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
	"time"
)

type CreateTenantCommand struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// APIKeyHash must be computed by the caller (internal/api) before
	// proposing this command, never generated here: Apply runs
	// independently on every replica, so any randomness generated inside
	// it (e.g. a fresh key) would diverge between nodes. The hash is part
	// of the replicated payload precisely so every replica applies the
	// same value.
	APIKeyHash string `json:"api_key_hash"`
}

func ApplyCreateTenant(tx storage.Tx, cmd CreateTenantCommand, proposedAt time.Time) (*model.Tenant, error) {
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	if cmd.APIKeyHash == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "api_key_hash", Message: "api_key_hash is missing"}
	}
	existing, err := tx.GetTenant(cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("read failed: %v", err)}
	}
	if existing != nil {
		return nil, &CommandError{Kind: KindConflict, Field: "id", Message: "tenant with this ID already exists"}
	}
	tenant := model.Tenant{
		ID:         cmd.ID,
		Name:       cmd.Name,
		CreatedAt:  proposedAt,
		APIKeyHash: cmd.APIKeyHash,
	}
	if err := tx.PutTenant(&tenant); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &tenant, nil
}
