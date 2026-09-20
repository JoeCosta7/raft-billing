package config

import (
	"strings"
	"testing"
)

func baseArgs() []string {
	return []string{
		"-node-id=n1",
		"-raft-addr=127.0.0.1:8300",
		"-http-addr=127.0.0.1:8400",
		"-data-dir=/tmp/data",
		"-peers=n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003",
	}
}

func withoutFlag(args []string, prefix string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		if !strings.HasPrefix(a, prefix) {
			out = append(out, a)
		}
	}
	return out
}

func TestNew(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		noAdminKey bool   // if true, leave SCHEDULER_ADMIN_KEY unset instead of the default test value
		wantErr    string // substring expected in error; empty means no error
		check      func(t *testing.T, cfg *Config)
	}{
		{
			name: "happy path",
			args: baseArgs(),
			check: func(t *testing.T, cfg *Config) {
				if cfg.AdminKey != "test-admin-key" {
					t.Errorf("AdminKey = %q, want %q", cfg.AdminKey, "test-admin-key")
				}
				if cfg.NodeID != "n1" {
					t.Errorf("NodeID = %q, want %q", cfg.NodeID, "n1")
				}
				if cfg.RaftAddr != "127.0.0.1:8300" {
					t.Errorf("RaftAddr = %q, want %q", cfg.RaftAddr, "127.0.0.1:8300")
				}
				if cfg.HTTPAddr != "127.0.0.1:8400" {
					t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, "127.0.0.1:8400")
				}
				if cfg.DataDir != "/tmp/data" {
					t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/data")
				}
				want := map[string]string{
					"n1": "127.0.0.1:7001",
					"n2": "127.0.0.1:7002",
					"n3": "127.0.0.1:7003",
				}
				if len(cfg.Peers) != len(want) {
					t.Fatalf("Peers = %v, want %v", cfg.Peers, want)
				}
				for id, addr := range want {
					if cfg.Peers[id] != addr {
						t.Errorf("Peers[%q] = %q, want %q", id, cfg.Peers[id], addr)
					}
				}
			},
		},
		{
			name:    "missing node-id",
			args:    withoutFlag(baseArgs(), "-node-id="),
			wantErr: "node-id is required",
		},
		{
			name:    "missing raft-addr",
			args:    withoutFlag(baseArgs(), "-raft-addr="),
			wantErr: "raft-addr is required",
		},
		{
			name:    "missing http-addr",
			args:    withoutFlag(baseArgs(), "-http-addr="),
			wantErr: "http-addr is required",
		},
		{
			name:    "missing data-dir",
			args:    withoutFlag(baseArgs(), "-data-dir="),
			wantErr: "data-dir is required",
		},
		{
			name:    "missing peers",
			args:    withoutFlag(baseArgs(), "-peers="),
			wantErr: "peers are required",
		},
		{
			name: "malformed peer entry missing =",
			args: []string{
				"-node-id=n1",
				"-raft-addr=127.0.0.1:8300",
				"-http-addr=127.0.0.1:8400",
				"-data-dir=/tmp/data",
				"-peers=n1,n2=127.0.0.1:8301",
			},
			wantErr: `malformed peer entry "n1"`,
		},
		{
			name: "node-id not in peers",
			args: []string{
				"-node-id=n1",
				"-raft-addr=127.0.0.1:8300",
				"-http-addr=127.0.0.1:8400",
				"-data-dir=/tmp/data",
				"-peers=n2=127.0.0.1:8300,n3=127.0.0.1:8301,n4=127.0.0.1:8302",
			},
			wantErr: `node-id "n1" not found in peers`,
		},
		{
			name: "bootstrap default",
			args: baseArgs(),
			check: func(t *testing.T, cfg *Config) {
				if cfg.Bootstrap != false {
					t.Errorf("Bootstrap = %v, want %v", cfg.Bootstrap, false)
				}
			},
		},
		{
			name: "bootstrap explicit true",
			args: append(baseArgs(), "-bootstrap"),
			check: func(t *testing.T, cfg *Config) {
				if cfg.Bootstrap != true {
					t.Errorf("Bootstrap = %v, want %v", cfg.Bootstrap, true)
				}
			},
		},
		{
			name:       "missing admin key",
			args:       baseArgs(),
			noAdminKey: true,
			wantErr:    adminKeyEnvVar + " environment variable is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.noAdminKey {
				t.Setenv(adminKeyEnvVar, "")
			} else {
				t.Setenv(adminKeyEnvVar, "test-admin-key")
			}
			cfg, err := New(tc.args)

			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("New() unexpected error: %v", err)
				}
				if tc.check != nil {
					tc.check(t, cfg)
				}
				return
			}

			if err == nil {
				t.Fatalf("New() expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("New() error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}
