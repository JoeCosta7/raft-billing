package model

import "time"

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	// APIKeyHash is the hex-encoded SHA-256 hash of this tenant's API key.
	// It needs a real JSON tag, not json:"-": storage.go's PutTenant/
	// GetTenant serialize this same struct via encoding/json for bbolt
	// persistence (there's no separate storage-vs-API representation in
	// this codebase), so json:"-" would silently drop it on every write —
	// it would never actually persist, only ever exist in memory for the
	// single request that created it. Keeping it out of HTTP responses is
	// the API layer's job (see api.sanitizeTenant), not this tag's.
	APIKeyHash string `json:"api_key_hash"`
}
