# L3 Sampler Startup Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent a predictable pre-mapping startup race from emitting a false
`evidence_writer_failed` event while preserving strict sampling failures after
the five-pair input exists.

**Architecture:** `run_sampler()` remains boot-grid anchored and reads the
existing immutable runtime snapshot at each eligible slot. It skips only slots
whose desired membership cardinality is not exactly ten; the existing
collection, append, event, and verdict paths are unchanged after that gate.

**Tech Stack:** Python 3.12, asyncio, pytest, Loguru, asyncpg/PostgreSQL, Fly.io.

## Global Constraints

- Desired membership must be exactly ten tokens before sampling starts.
- Do not wait for committed/evidenced convergence; failed convergence remains
  an ordinary durable health sample.
- Do not add a disallowed-event exception or change AcceptanceConfig.
- Preserve the boot grid, exact future T0 semantics, and all cumulative
  coverage rules.
- No schema, credential, trading, H-009, retention, or chaos mutation.

---

### Task 1: Gate startup sampling by desired membership

**Files:**
- Modify: `src/polyarb/observation/l3_sampler.py:373`
- Test: `tests/m1-perception/test_l3_evidence_sampler.py:392`

**Interfaces:**
- Consumes: `runtime.snapshot().desired: frozenset[str]`
- Produces: unchanged `run_sampler(...) -> None`; no new public interface

- [ ] **Step 1: Write the failing startup-race test**

Add this scheduler test:

```python
async def test_run_sampler_skips_slots_until_exact_desired_mapping_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime()
    clock = {"now": START}
    calls: list[tuple[int, datetime]] = []
    stop_event = asyncio.Event()

    async def _sample_once(**kwargs):
        calls.append((kwargs["sample_seq"], kwargs["scheduled_at"]))
        stop_event.set()
        return True

    async def _wait_for_stop(_stop_event, delay_s):
        assert delay_s == pytest.approx(30.0)
        _publish_current_membership(runtime, _pairs())
        clock["now"] = START + timedelta(seconds=30)
        return False

    monkeypatch.setattr(l3_sampler, "_utc_now", lambda: clock["now"])
    monkeypatch.setattr(l3_sampler, "_wait_for_stop", _wait_for_stop)
    monkeypatch.setattr(l3_sampler, "sample_once", _sample_once)

    await l3_sampler.run_sampler(
        stop_event,
        settings=_settings(),
        ws_consumer=_ConsumerWithoutMembershipReads(),
        reconciliation_state=_reconciliation(),
        runtime=runtime,
        store=SimpleNamespace(),
    )

    assert calls == [(1, START + timedelta(seconds=30))]
    assert runtime.peek_pending_event() is None
```

Publish current membership in the two existing scheduler tests that expect seq
0 to run:

```python
runtime = _runtime()
_publish_current_membership(runtime, _pairs())
```

- [ ] **Step 2: Run the new test and prove RED**

Run:

```bash
uv run pytest tests/m1-perception/test_l3_evidence_sampler.py::test_run_sampler_skips_slots_until_exact_desired_mapping_exists -q
```

Expected: FAIL because seq 0 calls `sample_once()` before desired membership is
published.

- [ ] **Step 3: Implement the narrow gate**

Immediately after the slot-window check and before assigning `sample_seq`, add:

```python
        if len(runtime.snapshot().desired) != 10:
            next_boundary_index = boundary_index + 1
            continue
```

Update the docstring to state that elapsed and pre-mapping slots are skipped.

- [ ] **Step 4: Prove GREEN and unchanged failure semantics**

Run:

```bash
uv run pytest tests/m1-perception/test_l3_evidence_sampler.py -q
uv run pytest tests/chaos/test_l3_evidence_chain.py -q
```

Expected: all tests pass, including the existing post-gate collection failure
and event-writer recovery cases.

- [ ] **Step 5: Run repository gates**

Run:

```bash
uv run pytest tests/alembic/test_007.py tests/m1-perception/test_l3_evidence.py tests/m1-perception/test_l3_evidence_store.py tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l3_evidence_sampler.py tests/m1-perception/test_l2_health_l3_subchecks.py tests/m1-perception/test_l3_soak_verdict.py tests/m1-perception/test_l3_evidence_cli.py tests/chaos/test_l3_evidence_chain.py -q -rs
uv run pytest -q -rs
uv run ruff check src/polyarb/observation/l3_sampler.py tests/m1-perception/test_l3_evidence_sampler.py
uv run python -m compileall -q src/polyarb scripts
make docs-m1-check
make planning-status
```

Expected: all commands exit 0; the repository-wide legacy Ruff baseline is not
expanded.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/observation/l3_sampler.py \
  tests/m1-perception/test_l3_evidence_sampler.py
git commit -m "fix(05.4): gate sampler until desired mapping exists"
```

### Task 2: Reject the raced boot and prove a clean exact-SHA replacement

**Files:**
- Modify:
  `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`

**Interfaces:**
- Consumes: exact Git SHA from `git rev-parse HEAD`
- Produces: one new Fly/DB boot eligible for readiness and unique A5

- [ ] **Step 1: Preserve the rejected boot**

Record boot `ba6630c2-5ca9-49b2-a0c0-947bff9d1f03`, workflow
`30090267465`, exact SHA
`7c014613d9c27fe4b9eec2f672acde5e7046d24e`, and event seq 0
`evidence_writer_failed / sample_collection_failed`. State that the boot is
permanently ineligible for readiness/A5.

- [ ] **Step 2: Push and re-prove the production boundary**

Push `main`, then run the existing Keychain-derived, non-printing DSN workflow:

```bash
make supabase-prod-revision expected_ref=zoqsmjeejfkrokwttjbx expected_revision=007
make l3-runtime-credential-check expected_ref=zoqsmjeejfkrokwttjbx
make l3-retention-operator-check expected_ref=zoqsmjeejfkrokwttjbx
```

Require Fly secret inventory to contain `POLYARB_L2_RUNTIME_DB_DSN` and exclude
`POLYARB_SUPABASE_DB_DSN` plus `L3_RETENTION_DSN`.

- [ ] **Step 3: Deploy and cross-check exact identity**

```bash
make deploy-l2-prod
```

Require the workflow `headSha`, Fly `GH_SHA`, `POLYARB_RELEASE_ID`, image
digest, machine ID/instance, and latest database boot to agree with:

```bash
git rev-parse HEAD
```

Any mismatch rejects the boot.

- [ ] **Step 4: Prove readiness**

On the new boot require:

- latest two promoter rows are `success/ok`, `5/10/10`;
- latest twelve health rows are contiguous, `pass/ok`, `10/10/10`, span at
  least 330 seconds, maximum gap at most 75 seconds;
- joined market rows are 60/60 pass with identities `5/5/5`;
- maximum book and OHLC source-observation ages are each below 120000 ms;
- mapping/config hashes are stable;
- disallowed event count is zero.

- [ ] **Step 5: Create A5 only after readiness**

Choose a unique future boot-grid boundary with at least 60 seconds of lead,
create the manifest with `O_EXCL`, bind it once before T0, and prove the exact
binding detail. Generate the declared T0 report only after `T0+30s`. If the
scheduled sample or report fails, retain A5 immutable and repeat with A6.

- [ ] **Step 6: Continue immutable checkpoints**

Run the manifest-declared T+6/T+12/T+18/T+24 reports only at or after their
not-before instants, then run final verify. Do not create Plan 05 SUMMARY or
close Phase 05.4 until all five canonical reports and final verification pass.
