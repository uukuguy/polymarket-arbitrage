# Task 5 Summary — Incident Recovery and Resource Isolation

Implemented the default-off Slice E control plane. Incidents are append-only,
deduplicated by active scope/kind and constrained to the approved lifecycle.
`verified` requires a component-specific writer mutation after recovery began:
an atomic Candidate success receipt binding the exact certified group, Quote
batch and terminal fact, an advancing validated Discovery batch, an advancing
validated Reconciliation checkpoint, or a bounded HTTP probe bound to the
expected release. Historical lifecycle and resource evidence is
replay-validated; corrupt or orphaned rows fail closed.

The durable resource controller reads actual Candidate freshness/count and
producer incident state. It pauses Reconciliation first, reduces Discovery
batch/duty next, then slows only normal Candidate cadence while preserving high
Candidate and HTTP. Empty Candidate expands Discovery without claiming health;
cooldown prevents recovery flapping. Candidate, Discovery and Reconciliation
consume the persisted decision.

Opportunity-first producers move out of the HTTP process when isolation is
enabled. The supervisor uses the exact shell-free commands, bounded/redacted
stdout/stderr tails, durable child receipts and producer-written heartbeats.
Stalls receive terminate→grace→kill; restart uses bounded exponential backoff
and escalates at the configured limit. Only a child-authenticated heartbeat
whose count, sequence and timestamp all strictly advance can extend the stall
deadline; read failures and status-marker changes cannot. Receipt history
enforces the exact outcome/exit-code matrix plus string/UTF-8/16 KiB output
tail bounds and integrity hashes. One component's receipt/incident does not
alter another component. Strict health reads the same durable facts via
`perception:open_incidents` and `perception:resource_mode`.
Pre-hash receipt schemas are upgraded transactionally: all legacy tails
validate before any hash backfill, invalid history rolls the migration back,
and repeated initialization is a no-op.

Verification: focused lifecycle/resource/subprocess fault tests and
proportional perception/health/daemon regressions passed; 2,625 repository
tests passed (one expected xfail and one skip). Ruff, compileall,
`make docs-m1-check`, `git diff --check` and `make planning-status` passed.
`python:3.12-slim` directly proved Python 3.12, asyncio subprocess,
terminate and kill primitives. The registry-specific L2 image check could not
pull its private Fly digest locally and is recorded as an external evidence
gap; no unavailable slim-image tools are used. No deployment or trading
behavior was added.
