# v3 Task 2 Report — Candidate-domain partition

## Status

Implemented and verified. Source/tests commit: `e04bf9f` (`fix(m1): partition classifier v3 candidate domain`).

## Implementation

- Added the v2-defaulted `classifier_contract` argument to the public and private fresh-projection readers, with closed v2/v3 validation before candidate work.
- Preserved the v2 classification path and its query budget; v3 alone bulk-loads raw events, exact immutable generation issues, and generation market/membership presence.
- Partitioned every v3 market candidate into exactly one member, closed-taxonomy exclusion, or existing diagnostic in the approved order.
- Partitioned event-only candidates in the approved order: raw event classification, ordinary event, identity/cardinality/conflict, current tradability, exact group exclusions, then exact quarantine.
- Both quarantine paths recompute the complete five-field issue tuple (`layer`, `category`, `market_id`, `detail`, `raw_payload`). Event-only quarantine additionally requires generation market and membership absence.
- Duplicate sidecar rows each remain diagnostic outcomes, preserving per-chunk candidate conservation; no catch-all exclusion was added.

## TDD evidence

Initial RED, before production edits:

```text
$ uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
    tests/m1-perception/test_structure_drift_classification.py \
    -k 'v3_market_candidate or v3_event_only_candidate or expected_exclusion'
FFFFFFFFFFFFFF                                                           [100%]
14 failed
TypeError: SQLiteStore.fetch_structure_drift_fresh_projection_chunk() got an unexpected keyword argument 'classifier_contract'
```

Self-review RED for a malformed ordinary-event lookalike:

```text
$ uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
    -k contradictory_ordinary
F                                                                        [100%]
1 failed
AssertionError: ['non-neg-risk-event-member'] == []
```

Final focused GREEN:

```text
$ uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
    tests/m1-perception/test_structure_drift_classification.py \
    -k 'v3_market_candidate or v3_event_only_candidate or expected_exclusion'
...............                                                          [100%]
```

Final regression GREEN:

```text
$ uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
    tests/m1-perception/test_structure_drift_classification.py
..........................................................
```

Static verification:

```text
$ uv run ruff check src/polyarb/storage/sqlite_store.py \
    tests/m1-perception/test_structure_drift_projection.py \
    tests/m1-perception/test_structure_drift_classification.py
All checks passed!
$ git diff --check -- src/polyarb/storage/sqlite_store.py \
    tests/m1-perception/test_structure_drift_projection.py \
    tests/m1-perception/test_structure_drift_classification.py
# exit 0, no output
```

## Files

- `src/polyarb/storage/sqlite_store.py`
- `tests/m1-perception/test_structure_drift_projection.py`
- `tests/m1-perception/test_structure_drift_classification.py`
- `.superpowers/sdd/v3-task-2-report.md`

## Self-review

- Confirmed non-boolean market `negRisk`, contradictory event classification, invalid member booleans, duplicate identity, global conflict, unknown unsupported reason, missing issue, and one-byte issue tampering all remain diagnostic.
- Confirmed ordinary/event tradability exclusions occur only after their required earlier validations.
- Confirmed exact quarantine matching cannot be inferred from payload shape or a partial issue match.
- Confirmed default v2 callers produce empty exclusions and all pre-existing projection/classification tests remain green.

## Concerns

None blocking. V3 performs four additional bounded bulk queries per non-empty chunk for authenticated evidence; v2 performs none of them.
