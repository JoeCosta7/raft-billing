package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

// Config stuff -- upcase names get imported don't forget
type Config struct {
	NodeID    string
	RaftAddr  string
	HTTPAddr  string
	DataDir   string
	Peers     map[string]string
	Bootstrap bool
	// AdminKey authorizes the two inherently cross-tenant HTTP endpoints
	// (create a tenant, list all tenants) — read from an environment
	// variable rather than a CLI flag, deliberately unlike every other
	// field here: it's a secret, and flags are visible via process listing
	// on most systems in a way environment variables at least aren't by
	// default. Every node in a cluster must be configured with the same
	// value.
	AdminKey string

	peersRaw string
}

const adminKeyEnvVar = "SCHEDULER_ADMIN_KEY"

func New(args []string) (*Config, error) {
	cfg := &Config{}
	fs := flag.NewFlagSet("scheduler", flag.ContinueOnError)
	fs.StringVar(&cfg.NodeID, "node-id", "", "Id for a node")
	fs.StringVar(&cfg.RaftAddr, "raft-addr", "", "raft listen address")
	fs.StringVar(&cfg.HTTPAddr, "http-addr", "", "http listen address")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "path to data directory")
	fs.StringVar(&cfg.peersRaw, "peers", "", "comma-separated list of node-id=addr peers")
	fs.BoolVar(&cfg.Bootstrap, "bootstrap", false, "bootstraps as the first node in the cluster")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	cfg.AdminKey = os.Getenv(adminKeyEnvVar)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (cfg *Config) validate() error {
	if cfg.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}
	if cfg.RaftAddr == "" {
		return fmt.Errorf("raft-addr is required")
	}
	if cfg.HTTPAddr == "" {
		return fmt.Errorf("http-addr is required")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if cfg.AdminKey == "" {
		return fmt.Errorf("%s environment variable is required", adminKeyEnvVar)
	}
	if err := cfg.parsePeers(); err != nil {
		return err
	}
	if _, found := cfg.Peers[cfg.NodeID]; !found {
		return fmt.Errorf("node-id %q not found in peers", cfg.NodeID)
	}
	return nil
}

func (cfg *Config) parsePeers() error {
	if cfg.peersRaw == "" {
		return fmt.Errorf("peers are required")
	}
	peers := make(map[string]string)
	for entry := range strings.SplitSeq(cfg.peersRaw, ",") {
		entry = strings.TrimSpace(entry)
		id, addr, ok := strings.Cut(entry, "=")
		if !ok {
			return fmt.Errorf("malformed peer entry %q, want node-id=addr", entry)
		}
		id = strings.TrimSpace(id)
		addr = strings.TrimSpace(addr)
		if id == "" {
			return fmt.Errorf("malformed peer entry %q, empty node-id", entry)
		}
		if addr == "" {
			return fmt.Errorf("malformed peer entry %q, empty addr", entry)
		}
		if _, dup := peers[id]; dup {
			return fmt.Errorf("duplicate peer node-id %q", id)
		}
		peers[id] = addr
	}
	cfg.Peers = peers
	return nil
}
