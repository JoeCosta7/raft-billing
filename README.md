# raft-biling

A distributed, multi-tenant task scheduler in Go, built on [HashiCorp Raft](https://github.com/hashicorp/raft) for consensus. Every state transition goes through a single Raft-replicated log applied by one state machine, so replicas can never disagree about what already happened.

It dispatches HTTP callbacks on a schedule (one-time or recurring: interval, daily, weekly, monthly), retries them with exponential backoff, tracks per-attempt audit records, and recovers cleanly across leadership changes.

## Requirements

- Go 1.26+

## Running a single node

```sh
go build -o scheduler ./cmd/scheduler

SCHEDULER_ADMIN_KEY=change-me ./scheduler \
  --node-id=node1 \
  --raft-addr=127.0.0.1:7001 \
  --http-addr=127.0.0.1:8001 \
  --data-dir=/tmp/scheduler-node1 \
  --peers=node1=127.0.0.1:7001 \
  --bootstrap
```

`--bootstrap` initializes a brand-new cluster and should only be passed on the first node the first time it starts. `--peers` is a comma-separated `node-id=raft-addr` list of every voting member (raft addresses, not HTTP addresses).

`SCHEDULER_ADMIN_KEY` is required on every node and must be identical across the cluster — it's the admin credential (see [Authentication](#authentication) below). It's an env var rather than a flag deliberately, since flags are visible in a process listing.

## Running a 3-node cluster

Start three nodes with the same `--peers` list, one `--bootstrap`ed:

```sh
SCHEDULER_ADMIN_KEY=change-me ./scheduler --node-id=node1 --raft-addr=127.0.0.1:7001 --http-addr=127.0.0.1:8001 \
  --data-dir=/tmp/scheduler-node1 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003 \
  --bootstrap

SCHEDULER_ADMIN_KEY=change-me ./scheduler --node-id=node2 --raft-addr=127.0.0.1:7002 --http-addr=127.0.0.1:8002 \
  --data-dir=/tmp/scheduler-node2 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003

SCHEDULER_ADMIN_KEY=change-me ./scheduler --node-id=node3 --raft-addr=127.0.0.1:7003 --http-addr=127.0.0.1:8003 \
  --data-dir=/tmp/scheduler-node3 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003
```

All nodes must be started with the same `SCHEDULER_ADMIN_KEY` — it's not itself replicated through Raft, so a mismatched value on one node just means that node's admin endpoints reject an otherwise-valid admin key.

Only the leader serves API requests. A non-leader node responds `503` 

## Authentication

Every request needs an `Authorization: Bearer <token>` header. Two kinds of credential exist:

- **Admin key** (`SCHEDULER_ADMIN_KEY`) — authorizes the two inherently cross-tenant endpoints, `POST /tenants` and `GET /tenants`, and can also act on any single tenant's endpoints.
- **Tenant API key** — scoped to one tenant. `POST /tenants` returns a freshly generated key in the response body's `api_key` field; that's the only time it's ever shown, since only its SHA-256 hash is persisted. A tenant's key only ever authorizes that tenant's own `/tenants/{tenantID}/...` resources, never another tenant's, even against a path that names one.

There's no built-in TLS termination — this assumes the service runs behind a TLS-terminating proxy or load balancer, so a bearer token is never sent over a plaintext connection in practice.

## API


| Method | Path | |
|---|---|---|
| `POST` | `/tenants` | Create a tenant |
| `GET` | `/tenants` | List tenants |
| `GET` | `/tenants/{tenantID}` | Get a tenant |
| `POST` | `/tenants/{tenantID}/schedules` | Create a schedule |
| `GET` | `/tenants/{tenantID}/schedules/{scheduleID}` | Get a schedule |
| `POST` | `/tenants/{tenantID}/schedules/{scheduleID}/pause` | Pause a schedule |
| `POST` | `/tenants/{tenantID}/schedules/{scheduleID}/cancel` | Cancel a schedule |
| `POST` | `/tenants/{tenantID}/schedules/{scheduleID}/resume` | Resume a paused schedule |
| `GET` | `/tenants/{tenantID}/schedules/{scheduleID}/executions` | List executions for a schedule |
| `GET` | `/tenants/{tenantID}/executions?status=` | List executions by status |
| `GET` | `/tenants/{tenantID}/executions/{executionID}` | Get an execution |
| `GET` | `/tenants/{tenantID}/executions/{executionID}/attempts` | List attempts for an execution |
| `GET` | `/tenants/{tenantID}/attempts/{attemptID}` | Get an attempt |

Error responses are `{"error": "..."}` with a status mapped from the rejection kind: `400` validation, `401` missing/invalid credentials, `404` not found, `409` conflict, `429` rate limited, `503` not leader / unreachable, `500` storage.

### Pagination

The four list endpoints (`GET /tenants`, the two execution-list endpoints, and the attempts list) take `?limit=` (default 50, max 200) and `?cursor=` query params and respond with an envelope instead of a bare array:

```json
{"items": [...], "next_cursor": "01J..."}
```

`next_cursor` is omitted once there's no next page. Treat it as opaque — always pass back exactly what the previous response gave you, don't construct one yourself. `limit` outside `1..200`, or non-integer, is a `400`.

### Rate limiting

Every request draws from a token bucket (20 req/s, burst 40) keyed by whichever credential authenticated it: each tenant's own API key gets its own independent budget, while the admin key shares one budget across every endpoint it touches, regardless of which tenant's path it's used on. Exceeding it gets a `429` with a `Retry-After` header.

This budget is in-memory on whichever node is currently leader, not replicated through Raft — a leadership failover resets it. That's a deliberate tradeoff: rate-limit counters have no business going through consensus, and since only the leader ever serves API requests anyway, there's nothing to coordinate across nodes in the first place.

### Example: create a recurring schedule

```sh
curl -X POST http://127.0.0.1:8001/tenants \
  -H "Authorization: Bearer change-me" \
  -d '{"id":"example","name":"Fake Corp"}'
# => {"id":"example","name":"Fake Corp","created_at":"...","api_key":"sched_..."}
# save that api_key now — it's never shown again

curl -X POST http://127.0.0.1:8001/tenants/example/schedules \
  -H "Authorization: Bearer sched_..." \
  -d '{
  "id": "daily-report",
  "callback_url": "https://example.com/webhook",
  "payload": {"report": "daily"},
  "schedule_type": "recurring",
  "first_run_at": "2026-01-01T09:00:00Z",
  "recurrence": {"cadence": "daily"},
  "timezone": "America/New_York",
  "max_attempts": 5,
  "retry_backoff": {"initial": 30000000000, "multiplier": 2, "max": 3600000000000}
}'
```

Every dispatched callback carries `X-Scheduler-Idempotency-Key` and `X-Scheduler-Attempt-Id` headers so a receiver can deduplicate a retried or double-delivered request.

## Architecture

- `internal/model` — domain types (`Tenant`, `Schedule`, `Execution`, `Attempt`) and recurrence math.
- `internal/command` — the full set of state transitions 
- `internal/storage` — bbolt-backed persistence with secondary indexes (by status, by schedule, by tenant) for the query patterns the scheduler and API need.
- `internal/statemachine` — the Raft `FSM`: `Apply` runs `command.Dispatch` inside a storage transaction; `Snapshot`/`Restore` back a real bbolt snapshot, not a custom serialization.
- `internal/raftnode` — wraps `*raft.Raft`: leader-gated reads, `Propose` for writes, and a leadership-change notification channel.
- `internal/scheduler` — the `Worker`/`Supervisor` dispatch loop: scans for due schedules, claims and fires executions, retries with backoff, and — on leadership change — reclassifies and recovers any in-flight executions left behind by the previous leader.
- `internal/api` — the HTTP surface over all of the above.
- `cmd/scheduler` — wires everything together and runs it.

## Testing

```sh
go test ./...
```

