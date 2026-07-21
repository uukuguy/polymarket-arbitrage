# M1 Continuous L3 Soak Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or the GSD execute-phase workflow to implement this plan task-by-task. Steps use checkbox state in the five authoritative GSD plans.

**Goal:** Make the claim “five L3 markets remained healthy throughout one strict 24-hour window” mechanically provable from durable exact-window evidence.

**Architecture:** One boot-scoped runtime is the sole health/sampler membership truth; WsConsumer synchronously publishes every desired/control/generation/evidence transition into it. PostgreSQL supplies server `recorded_at`, append-only triggers, exact raw-row reads, and cleanup executable only through a dedicated retention credential. One AcceptanceConfig digest and a unique future-T0 manifest bound before its T0 bind identity/config/bounds; `build_soak_report(evidence, manifest, start, end, require_24h)` makes that contract explicit, and five canonical JSON reports carry raw-row digests mechanically re-queried by the final verifier.

**Tech Stack:** Python 3.12, asyncio, asyncpg, PostgreSQL/Supabase, Alembic 007, Starlette health endpoints, pytest/Testcontainers, Ruff, uv, Fly.io, Make.

## Global Constraints

- Requirements PHASE05.4-R01 through PHASE05.4-R08 are mandatory in every authoritative plan.
- TDD is mandatory: eligible logic follows RED -> GREEN -> REFACTOR with one independently reviewable test cycle per task.
- Security defaults to ASVS L1; any unresolved High threat blocks the current plan.
- Preserve strict N=5, ten distinct Yes/No tokens, the 300-second promoter cadence, 30-second sampler cadence, 75-second maximum sample gap, 360-second maximum scheduled-to-start gap, and the existing `<120s` strict book/OHLC freshness boundary.
- Every new executable path is routed through Makefile; status/checkpoint/verify/retention commands are read-only.
- Local success never authorizes production. Alembic 007 and deployment require the explicit Plan 05 user gate.
- Migration and deployment require two distinct exact approvals and allowlisted production-ref/revision proof.
- Disallowed runtime events fail by kind, not severity; exceptions must be immutable pre-T0 manifest input.
- The release-37 window beginning `2026-07-21T14:29:47.941Z` is permanently diagnostic-only.
- After each plan's code/evidence commit, create `05.4-0N-SUMMARY.md`, commit it, and require `make planning-status` OK before the next plan.

---

## Authoritative Plans

Detailed executable XML tasks live only in these files:

1. [05.4-01-PLAN.md](../../../.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-01-PLAN.md) — evidence schema, runtime/boot/promoter records, storage boundary, blocking local migration replay.
2. [05.4-02-PLAN.md](../../../.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-02-PLAN.md) — desired/committed/evidenced membership, book-depth truth, terminal promoter transaction.
3. [05.4-03-PLAN.md](../../../.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-03-PLAN.md) — 30-second atomic sampler, runtime events, four strict health chains.
4. [05.4-04-PLAN.md](../../../.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-04-PLAN.md) — exact-window verdict/CLI, four required Make targets, local chaos, deploy contract, teaching/manual.
5. [05.4-05-PLAN.md](../../../.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-05-PLAN.md) — separately authorized production migration/deploy/readiness and fresh strict 24-hour soak.

## File Map

| File | Responsibility | Plan |
|---|---|---:|
| `alembic/versions/007_l3_soak_evidence.py` | Five evidence tables, server timestamps, append-only triggers/grants, protected retention function | 01 |
| `src/polyarb/observation/l3_evidence.py` | Enums, AcceptanceConfig, frozen records, canonical hashes, synchronized runtime state | 01-03 |
| `src/polyarb/storage/l3_evidence_store.py` | Append/read-only daemon store plus isolated dedicated-role protected-function retention client | 01-04 |
| `src/polyarb/daemon/ws_consumer.py` | Desired/committed/evidenced membership and all-required-token generation barrier | 02-03 |
| `src/polyarb/observation/l3_promote.py` | Boot-anchored scheduled tick and one typed terminal promoter transaction | 02 |
| `src/polyarb/observation/l3_sampler.py` | Atomic 30-second process/five-market collection and event writer | 03 |
| `src/polyarb/daemon/l2_main.py` | Shared boot/store/runtime lifecycle and bounded task composition | 02-03 |
| `src/polyarb/http/l2_health.py` | Sample, ledger, membership, and worst-market chain-truth checks | 03 |
| `src/polyarb/observation/l3_soak_verdict.py` | Pure expected-tick/gap/cardinality/freshness/event/coverage aggregation | 04 |
| `scripts/l3_evidence.py` | Read-only status/checkpoint/verify/retention CLI | 04 |
| `scripts/chaos_l3_evidence.py` | Local/Testcontainer end-to-end chain fault harness | 04 |
| `Makefile` | Four required operator targets plus local chain-chaos entry | 04 |
| `.github/workflows/deploy-l2.yml` | Complete L2 observation/storage/migration path trigger contract | 04 |
| `docs/learning/22-L3连续浸泡证据.md` | Mental model, tradeoffs, file:line chains, self-checks, FAQ | 04 |
| `05.4-SOAK-MANIFEST-*.json`, attempt-unique `{T0,T6,T12,T18,T24}-REPORT.json`, `05.4-SOAK-LOG.md` | Pre-T0 DB-bound immutable attempts, selected canonical raw-row reports, and final verdict | 05 |

## Architecture and Dependencies

```text
Alembic 007 + typed store (01)
            |
            v
WS desired/committed/evidenced + promoter terminal transaction (02)
            |
            v
30s 1+5 sampler + runtime events + strict health chains (03)
            |
            v
manifest + exact-window/raw-row verdict + Make/chaos/deploy/docs (04)
            |
            v
separate migration/retention-credential/deploy approvals -> readiness -> future manifest/T0 -> T6/T12/T18/T24 (05)
```

The sequence is intentionally fully serialized: each plan consumes the previous SUMMARY and `make planning-status` gate. Plan 05 is the only production-mutating plan and is `autonomous: false`.

## Test Cadence

- Every eligible implementation task is marked TDD and executes RED -> GREEN -> REFACTOR before its atomic commit.
- Every plan runs the union of its focused suites, touched-file Ruff, and compile checks.
- Every wave runs `uv run pytest -q`; Plan 01 additionally requires a non-skipped PostgreSQL 007 upgrade/downgrade/upgrade replay.
- Plan 03 exercises each five-link fail-soft health chain end to end.
- Plan 04 runs local chain chaos plus `make chaos-l2-fly-image-check`; no production chaos endpoint is introduced.
- Plan 05 reruns all local gates before requesting production authority.

## Production Gates

1. Stop after local Plan 04; separately approve production migration for an exact allowlisted project ref.
2. Prove production revision 006, migrate once to 007, re-prove the same target at 007, and never downgrade. Separately approve/provision the dedicated retention operator credential; keep it out of Fly/repo/evidence and prove daemon EXECUTE denial.
3. Separately approve deployment for the exact verified SHA; bind release, machine/version, image ref/digest, boot, mapping, and AcceptanceConfig.
4. Require two successful promoter rows and twelve complete samples; select a future eligible sampler boundary with lead, create/bind a unique immutable manifest, prove its server binding precedes T0, then accept only the complete passing sample at exact T0. A failure permanently rejects that candidate and requires a new manifest/later T0.
5. Execute T0/T6/T12/T18/T24 as separate blocking tasks; each CLI enforces its manifest-derived not-before UTC and writes once.
6. Final verify reloads all five files and re-queries their raw rows. PASS only with one boot; <=75s; every <=360s successful 5/10/10 tick; locked freshness; no disallowed event kind; 5/10 book and five Yes-OHLC coverage. Any violation is permanently NOT-CLOSED.

## Exact Execution Command

```bash
/gsd-execute-phase 05.4 --ws m1-perception
```

The executor must follow Plans 01 -> 05 in order and stop at Plan 05's explicit production approval gate.
