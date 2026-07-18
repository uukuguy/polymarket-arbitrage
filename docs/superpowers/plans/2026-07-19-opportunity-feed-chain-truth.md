# Opportunity Feed Chain-Truth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the production neg-risk opportunity feed diagnostically truthful: distinguish valid zero results from stale data, endpoint failure, malformed payloads, and transport failure without weakening its executable-data freshness gate.

**Architecture:** A pure routing-layer classifier maps HTTP status plus response body to a bounded diagnostic record. A Typer command renders that record, while a curlrc-isolated Make target performs the only production GET and preserves non-2xx bodies. A dedicated H-008 climb profile verifies this contract; scheduler cadence, endpoint freshness threshold, database schema, and production configuration remain unchanged.

**Tech Stack:** Python 3.12, stdlib `json`/dataclasses, Typer, GNU Make, curl, pytest.

## Global Constraints

- Keep the opportunity route's 900-second fail-closed freshness limit.
- Production entry is exactly one `curl --disable --request GET`; no retries, POST, Fly mutation, deployment, restart, secret, schema, migration, or chaos command.
- HTTP 503 is never normalized to zero opportunities.
- Canonical URL is `https://polyarb-l1.fly.dev/arbitrage/opportunities`; `URL=` only supports local test fixtures.
- Stdout is a stable JSON object. Exit 0 only for valid HTTP-200 feed payloads; every unavailable, invalid, or transport result exits 2.
- H-008 diagnoses the contract only. H-009 owns producer-cadence/executable-SLA alignment.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/polyarb/routing/opportunity_diagnosis.py` | Pure HTTP/body classifier and bounded output record. |
| `src/polyarb/cli_arbitrage.py` | `diagnose-feed` command reading captured body files. |
| `Makefile` | Read-only `diagnose-arb-feed-prod` production entry. |
| `tests/routing/test_opportunity_diagnosis.py` | Classification truth table. |
| `tests/cli/test_arbitrage_cli_process.py` | Process-level JSON and exit-code contract. |
| `tests/m1-perception/test_m1_manual_contract.py`, `tests/test_makefile.py` | Make safety, help, and docs synchronization. |
| `tools/climb/eval_local.py`, `tests/climb/test_eval_local.py` | Dedicated H-008 gates. |
| M1 manual, CURRENT, JOURNAL, climb state | Operator guidance and post-verification evidence. |

## Shared Interfaces

```python
DiagnosticKind = Literal[
    "available-zero", "available-opportunities", "stale-snapshot",
    "feed-unavailable", "invalid-response",
]

@dataclass(frozen=True)
class OpportunityFeedDiagnostic:
    kind: DiagnosticKind
    http_status: int
    reason: str
    count: int | None = None
    snapshot_age_seconds: float | None = None
    max_snapshot_age_seconds: float | None = None
    strategy: str | None = None
    profit_basis: str | None = None

    @property
    def exit_code(self) -> int: ...
    def to_dict(self) -> dict[str, object]: ...

def diagnose_opportunity_feed(
    http_status: int, body: str
) -> OpportunityFeedDiagnostic: ...
```

`diagnose-feed --http-status INTEGER --body-file PATH` prints
`diagnostic.to_dict()` and exits with `diagnostic.exit_code`. The Make recipe
owns curl transport failures and emits a fixed `transport-error` JSON object
with exit 2.

### Task 1: Pure feed classifier

**Files:**
- Create: `src/polyarb/routing/opportunity_diagnosis.py`
- Create: `tests/routing/test_opportunity_diagnosis.py`

**Consumes:** raw HTTP status and raw response text.  
**Produces:** `OpportunityFeedDiagnostic` and `diagnose_opportunity_feed()`.

- [ ] **Step 1: Write failing truth-table tests**

```python
def test_200_zero_is_the_only_zero_opportunity_result() -> None:
    result = diagnose_opportunity_feed(
        200,
        '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees",'
        '"count":0,"opportunities":[]}',
    )
    assert (result.kind, result.count, result.exit_code) == (
        "available-zero", 0, 0
    )

def test_503_snapshot_age_is_stale_not_zero() -> None:
    result = diagnose_opportunity_feed(
        503, '{"error":"snapshot age 1216.9s exceeds 900.0s"}'
    )
    assert result.kind == "stale-snapshot"
    assert result.snapshot_age_seconds == 1216.9
    assert result.max_snapshot_age_seconds == 900.0
    assert result.exit_code == 2

def test_unrelated_503_is_unavailable() -> None:
    result = diagnose_opportunity_feed(503, '{"error":"upstream unavailable"}')
    assert (result.kind, result.count, result.exit_code) == (
        "feed-unavailable", None, 2
    )
```

Also cover valid non-zero payload, malformed JSON, 200 payload missing/non-integer/negative `count`, non-list `opportunities`, count/list mismatch, and non-503 non-2xx. Assert reasons come from a fixed bounded vocabulary.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/routing/test_opportunity_diagnosis.py -q`  
Expected: collection error because the module does not exist.

- [ ] **Step 3: Implement minimal classifier**

Use `json.loads` and an anchored regex:
`^snapshot age (?P<age>\d+(?:\.\d+)?)s exceeds (?P<limit>\d+(?:\.\d+)?)s$`.

For HTTP 200 accept only an object with non-empty string `strategy`, non-empty string `profit_basis`, non-negative integer `count`, and list `opportunities` whose length equals `count`. Return `available-zero` only at zero; return `available-opportunities` otherwise. For 503, classify only the exact age error as `stale-snapshot`; every other non-success status is `feed-unavailable`. Invalid JSON or invalid success schema is `invalid-response`. `to_dict()` omits None values; never include raw server errors.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/routing/test_opportunity_diagnosis.py -q`  
Expected: all pass.

- [ ] **Step 5: Commit**

Run: `git add src/polyarb/routing/opportunity_diagnosis.py tests/routing/test_opportunity_diagnosis.py && git commit -m "feat(m2): classify opportunity feed availability"`

### Task 2: CLI JSON and exit contract

**Files:**
- Modify: `src/polyarb/cli_arbitrage.py`
- Modify: `tests/cli/test_arbitrage_cli_process.py`

**Consumes:** Task 1 classifier.  
**Produces:** `python -m polyarb.cli_arbitrage diagnose-feed --http-status N --body-file PATH`.

- [ ] **Step 1: Write failing process tests**

```python
def test_diagnose_feed_reports_zero_as_success(tmp_path) -> None:
    body = tmp_path / "zero.json"
    body.write_text(
        '{"strategy":"neg-risk-buy-all","profit_basis":"gross-before-fees",'
        '"count":0,"opportunities":[]}'
    )
    result = _cli("diagnose-feed", "--http-status", "200", "--body-file", str(body))
    assert result.returncode == 0
    assert json.loads(result.stdout)["kind"] == "available-zero"

def test_diagnose_feed_reports_stale_as_nonzero(tmp_path) -> None:
    body = tmp_path / "stale.json"
    body.write_text('{"error":"snapshot age 1200.0s exceeds 900.0s"}')
    result = _cli("diagnose-feed", "--http-status", "503", "--body-file", str(body))
    assert result.returncode == 2
    assert json.loads(result.stdout)["kind"] == "stale-snapshot"
```

Add missing-body coverage: exit 2 and a bounded stderr message that does not expose its path.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/cli/test_arbitrage_cli_process.py -k diagnose_feed -q`  
Expected: fails with no `diagnose-feed` command.

- [ ] **Step 3: Implement command**

Add a Typer command named `diagnose-feed`. Read UTF-8 body text; if reading fails, emit `opportunity diagnostic input unavailable: read error` to stderr and exit 2. Otherwise call Task 1, print `json.dumps(result.to_dict(), sort_keys=True)`, and exit exactly `result.exit_code`. Keep existing `scan` behavior untouched.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/cli/test_arbitrage_cli_process.py -k diagnose_feed -q`  
Expected: all new tests pass.

- [ ] **Step 5: Commit**

Run: `git add src/polyarb/cli_arbitrage.py tests/cli/test_arbitrage_cli_process.py && git commit -m "feat(m2): add opportunity feed diagnostic CLI"`

### Task 3: Read-only Make entry and operator contract

**Files:**
- Modify: `Makefile`, `scripts/check_m1_manual.py`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`, `tests/test_makefile.py`
- Modify: `docs/M1-市场感知平台使用手册.md`, `.planning/CURRENT.md`

**Consumes:** Task 2 command and canonical L1 URL.  
**Produces:** `make diagnose-arb-feed-prod [URL=...] [min_edge_bps=0]`.

- [ ] **Step 1: Write failing target/document tests**

```python
def test_opportunity_diagnosis_target_is_read_only_and_preserves_body() -> None:
    recipe = _make_recipe("diagnose-arb-feed-prod")
    assert "curl --disable --request GET" in recipe
    assert '-o "$$BODY" -w "%{http_code}"' in recipe
    assert "cli_arbitrage diagnose-feed" in recipe
    assert "polyarb-l1.fly.dev/arbitrage/opportunities" in recipe
    assert not re.search(
        r"\b(flyctl|POST|deploy|scale|restart|secret|schema|migrat|chaos)\b",
        recipe,
        re.I,
    )
```

Add assertions for Make help/dry-run, manual text containing
`make diagnose-arb-feed-prod`, `HTTP 503`, and `不是零机会`. Add an
index-guard regression: target recipe change without a valid manual sync record
must fail `scripts/check_m1_manual.py --staged`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_m1_manual_contract.py tests/test_makefile.py -k 'opportunity_diagnosis or diagnose_arb_feed' -q`  
Expected: absent target/docs assertions fail.

- [ ] **Step 3: Implement Make target and exact registry entry**

```make
## diagnose-arb-feed-prod: Explain fresh/zero/stale/unavailable opportunity-feed state. Read-only GET.
diagnose-arb-feed-prod:
	@URL="$(or $(URL),https://polyarb-l1.fly.dev/arbitrage/opportunities?min_edge_bps=$(or $(min_edge_bps),0))"; \
	BODY="$$(mktemp)"; trap 'rm -f "$$BODY"' EXIT; \
	HTTP_STATUS=$$(curl --disable --request GET -sS -o "$$BODY" -w "%{http_code}" "$$URL") || { \
	  printf '%s\n' '{"kind":"transport-error","reason":"request-failed"}'; exit 2; \
	}; \
	uv run python -m polyarb.cli_arbitrage diagnose-feed --http-status "$$HTTP_STATUS" --body-file "$$BODY"
```

Register only this target in the manual checker’s explicit M1 target registry.
Update manual production routing/troubleshooting with all five states, exit-0
semantics, and retained conditional readiness. Update CURRENT to say diagnosis
does not repair the producer-cadence/SLA mismatch.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/m1-perception/test_m1_manual_contract.py tests/test_makefile.py -q && make docs-m1-check`  
Expected: tests pass and output includes `M1 manual contract: OK`.

- [ ] **Step 5: Commit**

Run: `git add Makefile scripts/check_m1_manual.py tests/m1-perception/test_m1_manual_contract.py tests/test_makefile.py docs/M1-市场感知平台使用手册.md .planning/CURRENT.md && git commit -m "feat(m1): add read-only opportunity feed diagnosis"`

### Task 4: Dedicated H-008 evaluator and evidence

**Files:**
- Modify: `tools/climb/eval_local.py`, `tests/climb/test_eval_local.py`
- Modify after verified run: `docs/status/climb/hypotheses.yaml`, `runs.csv`, `session-state.json`, research trees, `.planning/JOURNAL.md`, and `05.2-05-SUMMARY.md`.

**Consumes:** Tasks 1–3 and manifest paradigm `opportunity-feed-chain-truth`.  
**Produces:** five H-008 gates and pending H-009 handoff.

- [ ] **Step 1: Write failing selector test**

```python
def test_opportunity_feed_chain_truth_profile_is_dedicated() -> None:
    commands = gate_commands_for({"paradigm": "opportunity-feed-chain-truth"})
    assert set(commands) == {"planning", "unit", "integration", "cli", "restart"}
    assert commands["unit"][-2:] == ["tests/routing/test_opportunity_diagnosis.py", "-q"]
    assert commands["cli"] == ["make", "docs-m1-check"]
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/climb/test_eval_local.py -k opportunity_feed_chain_truth -q`  
Expected: fails because it uses the legacy profile.

- [ ] **Step 3: Implement the profile and run H-008**

Add the profile only for `opportunity-feed-chain-truth`:

```python
{
  "planning": ["make", "planning-status"],
  "unit": ["uv", "run", "pytest", "tests/routing/test_opportunity_diagnosis.py", "-q"],
  "integration": ["uv", "run", "pytest", "tests/cli/test_arbitrage_cli_process.py", "-k", "diagnose_feed", "-q"],
  "cli": ["make", "docs-m1-check"],
  "restart": ["uv", "run", "pytest", "tests/m1-perception/test_m1_manual_contract.py", "-k", "opportunity_diagnosis", "-q"],
}
```

Preserve legacy and living-doc profiles. Run `make climb-cycle hypothesis=H-008`,
then exactly one `make diagnose-arb-feed-prod` read-only production check.
Record classification with timestamp and body-derived reason. Confirm H-008 only
when all gates score 100 and production classification agrees with the observed
response. Seed pending H-009 for producer cadence/SLA choice and regenerate the
tree by the existing deterministic path.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/climb/test_eval_local.py -q && make climb-cycle hypothesis=H-008`  
Expected: evaluator tests pass and all five H-008 subscores are 100 before confirmation.

- [ ] **Step 5: Commit**

Run: `git add tools/climb/eval_local.py tests/climb/test_eval_local.py docs/status/climb .planning/JOURNAL.md .planning/workstreams/m1-perception/phases/05.2-m1-platform-living-manual/05.2-05-SUMMARY.md && git commit -m "docs(climb): confirm opportunity feed diagnosis"`

## Final Verification

```bash
uv run pytest   tests/routing/test_opportunity_diagnosis.py   tests/cli/test_arbitrage_cli_process.py   tests/m1-perception/test_m1_manual_contract.py   tests/test_makefile.py   tests/climb/test_eval_local.py -q
make docs-m1-check
make planning-status
git diff --check
```

Expected: selected tests pass, manual contract is OK, planning has no drift, and no whitespace errors. No deployment, restart, push, or production configuration change is part of H-008.

