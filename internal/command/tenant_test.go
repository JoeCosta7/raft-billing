package command

import (
	"errors"
	"testing"
	"time"

	"raft-biling/internal/model"
)

func newTestCreateTenantCommand(opts ...func(*CreateTenantCommand)) *CreateTenantCommand {
	cmd := &CreateTenantCommand{
		ID:         testTenantID,
		Name:       "Default Tenant",
		APIKeyHash: "test-api-key-hash",
	}
	for _, o := range opts {
		o(cmd)
	}
	return cmd
}

func TestApplyCreateTenant_HappyPath(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateTenantCommand()
	proposedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tenant, err := ApplyCreateTenant(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyCreateTenant: unexpected error: %v", err)
	}
	if tenant == nil {
		t.Fatal("ApplyCreateTenant: returned nil tenant with nil error")
	}
	if tenant.ID != cmd.ID {
		t.Errorf("ID: got %q, want %q", tenant.ID, cmd.ID)
	}
	if tenant.Name != cmd.Name {
		t.Errorf("Name: got %q, want %q", tenant.Name, cmd.Name)
	}
	if !tenant.CreatedAt.Equal(proposedAt) {
		t.Errorf("CreatedAt: got %v, want %v", tenant.CreatedAt, proposedAt)
	}
	if tenant.APIKeyHash != cmd.APIKeyHash {
		t.Errorf("APIKeyHash: got %q, want %q", tenant.APIKeyHash, cmd.APIKeyHash)
	}

	stored, ok := tx.tenants[cmd.ID]
	if !ok {
		t.Fatalf("expected tenant persisted at key %q, but fake has no entry", cmd.ID)
	}
	if stored.Name != cmd.Name {
		t.Errorf("stored.Name: got %q, want %q", stored.Name, cmd.Name)
	}
	if stored.APIKeyHash != cmd.APIKeyHash {
		t.Errorf("stored.APIKeyHash: got %q, want %q", stored.APIKeyHash, cmd.APIKeyHash)
	}
}

func TestApplyCreateTenant_MissingAPIKeyHash(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateTenantCommand(func(c *CreateTenantCommand) { c.APIKeyHash = "" })

	tenant, err := ApplyCreateTenant(tx, *cmd, time.Time{})

	if tenant != nil {
		t.Errorf("expected nil tenant on error, got %+v", tenant)
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "api_key_hash" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "api_key_hash")
	}
}

func TestApplyCreateTenant_IDCollision(t *testing.T) {
	tx := newFakeTx()
	existing := &model.Tenant{
		ID:   testTenantID,
		Name: "Existing Tenant",
	}
	tx.tenants[existing.ID] = existing
	cmd := newTestCreateTenantCommand(func(c *CreateTenantCommand) { c.ID = existing.ID })

	tenant, err := ApplyCreateTenant(tx, *cmd, time.Time{})

	if tenant != nil {
		t.Errorf("expected nil tenant on error, got %+v", tenant)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}
