# Task 5 Implementer Report

Status: implemented and locally verified

## Scope

Task 5 only: exact-group Candidate CLOB fault injection at the authoritative
`read_group(group_id)` / selected `BooksReader.get_books(...)` boundary.
No global `BooksReader` or `ClobReaderClient` fault seam was added. Focused
collection, Discovery, Reconciliation, other groups and other priority lanes
remain outside this adapter. Nothing was deployed and no production state,
wallet, signing, balance or order path was touched.

The recovery proof required small typed extensions to the shared Task 4 fault
authority/runtime contract:

- `FaultRecoveryWriter.CANDIDATE_SUCCESS`;
- Candidate success-receipt validation against the current group revision,
  membership, quote batch, terminal fact, observed time and canonical receipt
  hash;
- `FaultRecoveryOutcome` (`RECORDED`, `INVALID`, `UNAVAILABLE`);
- owned `EVIDENCE_INVALID` terminalization for proven structural/semantic
  invalidity, kept separate from transient authority/DB unavailability.

## RED / GREEN evidence

The new test file was first invoked before it existed. Subsequent RED boundaries
were observed for each implementation slice:

- missing `CandidateBooksFault`;
- missing qualified Candidate Incident receipt type;
- `CandidateWatcher` rejecting `fault_runtime`;
- latency timeout creating an Incident without the exact injected `call_id`;
- lifecycle stopping at `cleaned` instead of writer-authenticated `recovered`;
- ambiguous pre-existing Incident abandoning rather than terminalizing invalid
  evidence;
- external cancellation leaving an injected call active;
- missing recovery proof not degrading;
- authority unavailability propagating instead of fail-open degradation.

The final focused run collected 260 tests and reached 100% with exit code 0:

```text
uv run pytest \
  tests/perception/test_candidate_fault_adapter.py \
  tests/perception/test_clob_incidents.py \
  tests/perception/test_candidate_watcher.py \
  tests/perception/test_fault_runtime.py \
  tests/perception/test_fault_control.py \
  tests/perception/test_fault_authority.py \
  tests/perception/test_gamma_fault_adapter.py \
  tests/perception/test_gamma_incidents.py -q
```

Relevant store tests collected 138 tests (156 unrelated tests deselected) and
reached 100% with exit code 0:

```text
uv run pytest tests/perception/test_store.py \
  -k 'candidate or fault or incident or recovery' -q
```

Schema lockstep plus untouched Discovery/Reconciliation regressions collected
262 tests and reached 100% with exit code 0:

```text
uv run pytest \
  tests/m1-perception/test_schema_lockstep.py \
  tests/perception/test_discovery.py \
  tests/perception/test_reconciliation.py -q
```

Ruff passed for all changed source and test files.

## Exact truth chain

1. The watcher reads the authoritative group revision before checking the
   group-keyed fault seam.
2. `CandidateBooksFault.before_books(group_id)` consumes only
   `clob-candidate-book-batch / group_id` and durably records the injection
   receipt before transforming the real CLOB call.
3. The selected priority-lane `BooksReader.get_books(..., projection="top")`
   remains the sole affected call.
4. `clob-429` raises `PolyApiException(status_code=429)`;
   `clob-missing-leg` removes only a real bounded result index and then follows
   the existing `QuoteCollectionIntegrityError` path; latency uses the bounded
   monotonic delay and the existing scheduler timeout boundary.
5. The watcher re-reads the group before publication. Drift, missing legs and
   failures publish neither a partial quote batch nor a cross-membership success
   receipt.
6. The qualified Candidate Incident writer binds the exact
   `candidate:<group_id>` scope, fault kind and injected `call_id`. A same-time
   organic/deduplicated Incident cannot masquerade as that detection.
7. Cleanup is persisted before a retry can prove recovery. Recovery then
   requires a newer atomic Candidate success receipt whose current group
   revision, membership, quote/fact rows, timestamp and canonical hash all
   validate in the same SQLite authority.
8. Proven semantic mismatch or tampering appends owned `EVIDENCE_INVALID` and
   freezes qualification. DB lock/I/O/authority exceptions freeze/degrade
   without inventing invalid evidence. Cancellation settles cleanup and
   re-raises the original cancellation.

## Remaining boundary

The relevant store slice was run rather than the entire 294-test store module;
the broader untouched producer regressions above cover Discovery and
Reconciliation. No full-repository suite or production image check was required
for this exact-group adapter task.
