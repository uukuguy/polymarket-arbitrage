## Task 7: Complete orchestrator, cleanup guarantees, and independent evaluator

**Files:**

- Modify: `scripts/perception_chaos.py`
- Modify: `scripts/perception_fault_readonly.py`
- Modify: `scripts/perception_fault_acceptance.py`
- Modify: `tests/m1-perception/test_perception_chaos_contract.py`
- Modify: `tests/m1-perception/test_perception_fault_acceptance.py`
- Create: `tests/m1-perception/test_upstream_fault_e2e.py`
- Modify: `Makefile`

**Step 1: Write RED lifecycle/orchestrator tests**

For all eight upstream faults, assert:

- execute remains blocked without exact release, target runtime, target key,
  separate authorization, and a new evidence directory;
- baseline must be green before arm;
- arm response binds exact fault/release/machine/boot/call-class/target;
- `try/finally` always calls cleanup after arm, including cancellation,
  detection timeout, malformed API response, evidence write failure, and
  `KeyboardInterrupt`;
- cleanup response proves data-plane clear before receipt persistence;
- `cleanup-failed` freezes the remaining matrix;
- orchestrator accepts exactly one expected Incident (or the explicitly
  modeled Gamma partial coverage fact);
- business recovery receipt is newer than injection and cleanup;
- resulting `evidence.json` contains a complete immutable fault history;
- fixture-generated evidence can test evaluator determinism but uses
  `scope=local-conformance`;
- evaluator rejects missing/duplicate/mismatched intent, injection, Incident,
  cleanup, recovery, runtime identity, event hash, or terminal verified event;
- evaluator cannot write to the source DB or call control endpoints.

Run:

```bash
uv run pytest \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py -q
```

Expected: FAIL.

**Step 2: Implement arm → observe → cleanup → recover**

For upstream faults, `perception_chaos.py` must:

1. collect the read-only green baseline;
2. resolve the current exact producer runtime;
3. create one canonical typed intent;
4. arm via the doubly signed control endpoint;
5. queue/wait for the matching component call;
6. observe injection and one authoritative Incident/coverage fact;
7. run cleanup in `finally`;
8. wait for the exact business recovery receipt;
9. export bounded read-only evidence; and
10. invoke no evaluator mutation.

Only after these paths pass tests, set `execute_supported=True` for:

```text
gamma-timeout gamma-partial gamma-malformed gamma-cursor
clob-missing-leg clob-429 clob-latency telegram-failure
```

Keep SQLite/disk/load/process/restart/deploy primitives separate.

**Step 3: Strengthen the independent evaluator**

For `scope=production-fault`, require:

```text
authorized → armed → injected → detected/coverage → contained
→ cleaned → recovered → verified
```

The evaluator must recompute every digest/hash and require exact:

- release/machine/boot;
- fault ID, call class, target digest, parameter digest, nonce digest;
- one injection;
- one Incident/coverage fact;
- cleanup before recovery;
- component-specific recovery table and row ID;
- zero open fault/Incident state;
- existing cross-membership, partial-publication, orphan-run, freshness, and
  reconciliation gates.

Any absent field is a named FAIL reason, never an implicit default.

**Step 4: Run focused and full local gates**

```bash
uv run pytest \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py -q
make test-m1-perception
make planning-status
```

Expected: PASS; no DRIFT.

**Step 5: Commit**

```bash
git add scripts/perception_chaos.py \
  scripts/perception_fault_readonly.py \
  scripts/perception_fault_acceptance.py \
  tests/m1-perception/test_perception_chaos_contract.py \
  tests/m1-perception/test_perception_fault_acceptance.py \
  tests/m1-perception/test_upstream_fault_e2e.py Makefile
git commit -m "feat(m1): qualify typed upstream fault lifecycles"
make planning-status
```

Expected: commit succeeds; no DRIFT.

---
