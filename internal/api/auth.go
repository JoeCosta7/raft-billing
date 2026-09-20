package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"strings"
)

// generateAPIKey returns a fresh random plaintext key and its hex-encoded
// SHA-256 hash. The hash is what travels through the replicated
// create_tenant command (see command.CreateTenantCommand.APIKeyHash for
// why); the plaintext is returned to the caller exactly once, in the HTTP
// response, and is never persisted anywhere.
func generateAPIKey() (plaintext, hash string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	plaintext = "sched_" + base64.RawURLEncoding.EncodeToString(buf)
	return plaintext, hashAPIKey(plaintext), nil
}

func hashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header, or reports false if it's missing or malformed.
func bearerToken(r *http.Request) (string, bool) {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}

// constantTimeEqual reports whether a and b are equal, in constant time.
// Empty a or b never matches anything, even another empty string — so an
// unconfigured secret (a zero-value Config.AdminKey, or a tenant with no
// key for some reason) can never accidentally "match" a missing or empty
// presented token; it fails closed.
func constantTimeEqual(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// requireAdmin answers 401 and returns false unless the request presents
// the configured admin key. Used only for the two endpoints that are
// inherently cross-tenant — create a tenant, list all tenants — which no
// per-tenant key could legitimately authorize.
func (a *API) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	token, ok := bearerToken(r)
	if !ok || !constantTimeEqual(token, a.cfg.AdminKey) {
		writeError(w, http.StatusUnauthorized, "missing or invalid admin credentials")
		return false
	}
	return true
}

// requireTenantAccess answers 401 and returns false unless the request
// presents either the admin key or the API key belonging to tenantID
// itself. Admin can act on any tenant; a tenant's own key only ever
// authorizes that tenant's own resources — never another tenant's, even if
// the path names one.
func (a *API) requireTenantAccess(w http.ResponseWriter, r *http.Request, tenantID string) bool {
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing or invalid credentials")
		return false
	}
	if constantTimeEqual(token, a.cfg.AdminKey) {
		return true
	}
	tenant, err := a.backend.GetTenant(tenantID)
	if err != nil {
		writeReadError(w, err)
		return false
	}
	if tenant == nil || !constantTimeEqual(hashAPIKey(token), tenant.APIKeyHash) {
		writeError(w, http.StatusUnauthorized, "missing or invalid credentials")
		return false
	}
	return true
}
