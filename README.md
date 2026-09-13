# raft-biling

A distributed, multi-tenant task scheduler in Go, built on [HashiCorp Raft](https://github.com/hashicorp/raft) for consensus. Every state transition goes through a single Raft-replicated log applied by one state machine, so replicas can never disagree about what already happened.

It dispatches HTTP callbacks on a schedule (one-time or recurring: interval, daily, weekly, monthly), retries them with exponential backoff, tracks per-attempt audit records, and recovers cleanly across leadership changes.

## Requirements

- Go 1.26+

## Running a single node

```sh
go build -o scheduler ./cmd/scheduler

./scheduler \
  --node-id=node1 \
  --raft-addr=127.0.0.1:7001 \
  --http-addr=127.0.0.1:8001 \
  --data-dir=/tmp/scheduler-node1 \
  --peers=node1=127.0.0.1:7001 \
  --bootstrap
```

`--bootstrap` initializes a brand-new cluster and should only be passed on the first node the first time it starts. `--peers` is a comma-separated `node-id=raft-addr` list of every voting member (raft addresses, not HTTP addresses).

## Running a 3-node cluster

Start three nodes with the same `--peers` list, one `--bootstrap`ed:

```sh
./scheduler --node-id=node1 --raft-addr=127.0.0.1:7001 --http-addr=127.0.0.1:8001 \
  --data-dir=/tmp/scheduler-node1 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003 \
  --bootstrap

./scheduler --node-id=node2 --raft-addr=127.0.0.1:7002 --http-addr=127.0.0.1:8002 \
  --data-dir=/tmp/scheduler-node2 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003

./scheduler --node-id=node3 --raft-addr=127.0.0.1:7003 --http-addr=127.0.0.1:8003 \
  --data-dir=/tmp/scheduler-node3 \
  --peers=node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003
```

Only the leader serves API requests. A non-leader node responds `503` 

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

Error responses are `{"error": "..."}` with a status mapped from the rejection kind: `400` validation, `404` not found, `409` conflict, `503` not leader / unreachable, `500` storage.

### Example: create a recurring schedule

```sh
curl -X POST http://127.0.0.1:8001/tenants -d '{"id":"example","name":"Fake Corp"}'

curl -X POST http://127.0.0.1:8001/tenants/example/schedules -d '{
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

