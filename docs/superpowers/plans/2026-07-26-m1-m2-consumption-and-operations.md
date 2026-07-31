# M1→M2 Consumption and Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert verified M1 candidates into safe M2 paper evaluations, expose the same facts in Dashboard, and make production incidents diagnosable without confusing a discovery window with final certification.

**Architecture:** A strict parser validates the M1 response and binds every candidate to source snapshot, universe hash, membership hash, and quote run. A dedicated buy-all evaluator computes a conservative paper result and never reuses the generic directional router, whose `<0.5 BUY / >=0.5 SELL` rule is incompatible with buying every YES leg. Dashboard server components consume the same L1 read models, while Polywatch persists incident reasons and the soak tool emits a diagnostic anomaly ledger separately from strict qualification.

**Tech Stack:** Python 3.12, dataclasses, Decimal, stdlib HTTP, Typer, pytest, Next.js 15, React 19, TypeScript 5.7, pnpm, Fly/Telegram monitoring, Make.

## Global Constraints

- Plans 1 and 2 are prerequisites.
- No real order, signing, allowance, wallet, or venue mutation is added.
- All buy-all legs are BUY YES; never route by whether ask is above or below 0.5.
- Candidate identity is immutable across one evaluation: source snapshot, universe hash, membership hash, and quote run ID must all remain unchanged.
- Top-level evaluation quantity is capped by the minimum ask size across every leg.
- Output separates gross profit, estimated costs, conservative net estimate, and paper eligibility.
- Dashboard and CLI consume the same M1 contracts; neither reimplements event completeness.
- Diagnostic windows continue collecting recoverable anomalies; strict windows retain zero-disallowed-event certification semantics.
- Every code task uses RED → GREEN TDD and ends in an atomic commit.

---

## File Structure

- Create `src/polyarb/routing/verified_candidate.py`: strict M1 response parser.
- Create `src/polyarb/routing/neg_risk_paper.py`: dedicated buy-all cost evaluation.
- Modify `src/polyarb/cli_arbitrage.py`: fetch/evaluate a real verified candidate without execution.
- Modify `Makefile`: `eval-arb-live`.
- Create `dashboard/lib/perception-api.ts`: typed server fetches from L1.
- Create `tests/m1-perception/test_dashboard_perception_contract.py`: route and
  safety-label contract test.
- Create Dashboard pages `/events/[event_id]`, `/markets/[market_id]`, `/opportunities`.
- Modify existing navigation/layout links.
- Modify `scripts/polywatch/healthz_watcher.py`: persist active/last incident reasons and timestamps.
- Create `src/polyarb/observation/l3_diagnostic_report.py`: non-certifying anomaly ledger.
- Modify manuals, learning documents, and focused tests.

### Task 1: Parse and validate verified M1 candidates

**Files:**
- Create: `src/polyarb/routing/verified_candidate.py`
- Test: `tests/routing/test_verified_candidate.py`

**Interfaces:**
- Produces `parse_verified_feed(payload: Mapping[str, object]) -> VerifiedFeed`.
- Produces `VerifiedFeed.find(group_id: str) -> VerifiedCandidate | None`.
- Raises only `CandidateContractError` with bounded reason codes.

- [ ] **Step 1: Write failing parser tests**

```python
def _payload() -> dict:
    return {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "verified-standard-neg-risk",
        "source_snapshot_id": 725,
        "universe_hash": "u" * 64,
        "quote_run_id": 300,
        "quote_sla_seconds": 300,
        "count": 1,
        "rejections": {"augmented-neg-risk-not-supported": 4},
        "opportunities": [{
            "event_id": "e1",
            "group_id": "g1",
            "membership_hash": "m" * 64,
            "quality": "complete-supported",
            "quote_run_id": 300,
            "quote_age_seconds": 10.0,
            "sum_asks": 0.95,
            "gross_edge_bps": 500.0,
            "executable_quantity": 8.0,
            "gross_profit": 0.4,
            "legs": [
                {"market_id": "m1", "condition_id": "c1", "slug": "one",
                 "yes_token_id": "t1", "ask_price": 0.40, "ask_size": 12.0},
                {"market_id": "m2", "condition_id": "c2", "slug": "two",
                 "yes_token_id": "t2", "ask_price": 0.55, "ask_size": 8.0},
            ],
        }],
    }


def test_parser_binds_feed_and_candidate_identity() -> None:
    feed = parse_verified_feed(_payload())
    candidate = feed.find("g1")
    assert candidate is not None
    assert candidate.identity == CandidateIdentity(
        source_snapshot_id=725,
        universe_hash="u" * 64,
        event_id="e1",
        group_id="g1",
        membership_hash="m" * 64,
        quote_run_id=300,
    )


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda p: p.update(coverage="known-universe"), "unverified-coverage"),
        (lambda p: p["opportunities"][0].update(quality="complete-unsupported"),
         "unsupported-quality"),
        (lambda p: p["opportunities"][0].update(quote_run_id=301),
         "mixed-quote-run"),
        (lambda p: p["opportunities"][0]["legs"].append(
            dict(p["opportunities"][0]["legs"][0])
        ), "duplicate-leg-identity"),
    ],
)
def test_parser_fails_closed_on_contract_drift(mutation, reason) -> None:
    payload = _payload()
    mutation(payload)
    with pytest.raises(CandidateContractError, match=reason):
        parse_verified_feed(payload)
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/routing/test_verified_candidate.py -q
```

Expected: module import failure.

- [ ] **Step 3: Implement strict immutable dataclasses**

```python
@dataclass(frozen=True)
class CandidateIdentity:
    source_snapshot_id: int
    universe_hash: str
    event_id: str
    group_id: str
    membership_hash: str
    quote_run_id: int


@dataclass(frozen=True)
class VerifiedLeg:
    market_id: str
    condition_id: str
    slug: str
    yes_token_id: str
    ask_price: Decimal
    ask_size: Decimal


@dataclass(frozen=True)
class VerifiedCandidate:
    identity: CandidateIdentity
    quote_age_seconds: Decimal
    sum_asks: Decimal
    gross_edge_bps: Decimal
    executable_quantity: Decimal
    gross_profit: Decimal
    legs: tuple[VerifiedLeg, ...]
```

Validate finite decimal values, unique market/condition/token identities, at
least two legs, exact `sum_asks`, exact minimum size, exact gross edge, matching
feed/candidate quote run, and quote age `<= quote_sla_seconds`. Reject bools
where integers are required.

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/routing/test_verified_candidate.py -q
uv run ruff check src/polyarb/routing/verified_candidate.py \
  tests/routing/test_verified_candidate.py
git add src/polyarb/routing/verified_candidate.py \
  tests/routing/test_verified_candidate.py
git commit -m "feat(m2): parse verified M1 candidates"
```

### Task 2: Evaluate buy-all candidates conservatively in paper mode

**Files:**
- Create: `src/polyarb/routing/neg_risk_paper.py`
- Test: `tests/routing/test_neg_risk_paper.py`

**Interfaces:**
- Produces `BuyAllPaperEvaluator.evaluate(candidate, requested_quantity) -> PaperEvaluation`.
- Does not call `RoutingEngine` or `ExecutionEngine`.

- [ ] **Step 1: Write failing evaluator tests**

```python
def test_every_leg_is_buy_yes_even_above_half_price(candidate) -> None:
    candidate = replace(
        candidate,
        legs=(
            replace(candidate.legs[0], ask_price=Decimal("0.40")),
            replace(candidate.legs[1], ask_price=Decimal("0.55")),
        ),
        sum_asks=Decimal("0.95"),
    )
    result = BuyAllPaperEvaluator(
        fee_bps=Decimal("50"),
        slippage_buffer_bps=Decimal("25"),
        minimum_net_edge_bps=Decimal("50"),
    ).evaluate(candidate, Decimal("5"))
    assert [(leg.action, leg.outcome) for leg in result.legs] == [
        ("BUY", "YES"), ("BUY", "YES")
    ]


def test_quantity_cannot_exceed_weakest_top_level(candidate) -> None:
    with pytest.raises(PaperEvaluationError, match="quantity-exceeds-top-level"):
        BuyAllPaperEvaluator.defaults().evaluate(
            candidate, candidate.executable_quantity + Decimal("0.000001")
        )


def test_net_estimate_separates_costs(candidate) -> None:
    result = BuyAllPaperEvaluator(
        fee_bps=Decimal("50"),
        slippage_buffer_bps=Decimal("25"),
        minimum_net_edge_bps=Decimal("50"),
    ).evaluate(candidate, Decimal("8"))
    assert result.gross_payout == Decimal("8")
    assert result.gross_cost == Decimal("7.60")
    assert result.fee_cost == Decimal("0.038")
    assert result.slippage_buffer == Decimal("0.019")
    assert result.estimated_net_profit == Decimal("0.343")
    assert result.paper_eligible is True
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/routing/test_neg_risk_paper.py -q
```

- [ ] **Step 3: Implement deterministic Decimal arithmetic**

```python
@dataclass(frozen=True)
class PaperLeg:
    market_id: str
    yes_token_id: str
    action: Literal["BUY"]
    outcome: Literal["YES"]
    quantity: Decimal
    limit_price: Decimal
    gross_cost: Decimal


@dataclass(frozen=True)
class PaperEvaluation:
    identity: CandidateIdentity
    legs: tuple[PaperLeg, ...]
    gross_payout: Decimal
    gross_cost: Decimal
    gross_profit: Decimal
    fee_cost: Decimal
    slippage_buffer: Decimal
    estimated_net_profit: Decimal
    estimated_net_edge_bps: Decimal
    paper_eligible: bool
    rejection_reason: str | None
```

Use:

```python
gross_cost = quantity * candidate.sum_asks
gross_payout = quantity
fee_cost = gross_cost * self.fee_bps / Decimal(10_000)
slippage = gross_cost * self.slippage_buffer_bps / Decimal(10_000)
net = gross_payout - gross_cost - fee_cost - slippage
net_edge_bps = net / gross_cost * Decimal(10_000)
```

`paper_eligible` requires positive net and
`net_edge_bps >= minimum_net_edge_bps`. This is an estimate, not a fill or order.

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/routing/test_neg_risk_paper.py \
  tests/routing/test_verified_candidate.py -q
uv run ruff check src/polyarb/routing/neg_risk_paper.py \
  tests/routing/test_neg_risk_paper.py
git add src/polyarb/routing/neg_risk_paper.py \
  tests/routing/test_neg_risk_paper.py
git commit -m "feat(m2): evaluate verified buy-all candidates"
```

### Task 3: Add a production candidate paper-evaluation command

**Files:**
- Modify: `src/polyarb/cli_arbitrage.py`
- Modify: `Makefile`
- Modify: `tests/cli/test_arbitrage_cli.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**
- Produces `evaluate-verified` CLI command.
- Produces `make eval-arb-live group_id=<id> quantity=<shares>`.
- Performs one read-only GET and no execution.

- [ ] **Step 1: Write failing CLI tests**

```python
def test_evaluate_verified_prints_identity_and_no_execution(
    runner, monkeypatch, tmp_path
) -> None:
    body = tmp_path / "feed.json"
    body.write_text(json.dumps(_verified_payload()))
    result = runner.invoke(app, [
        "evaluate-verified", "--body-file", str(body), "--group-id", "g1",
        "--quantity", "5", "--fee-bps", "50", "--slippage-buffer-bps", "25",
    ])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["mode"] == "paper-evaluation"
    assert payload["identity"]["group_id"] == "g1"
    assert payload["legs"][0]["action"] == "BUY"
    assert "execution" not in payload


def test_evaluate_verified_rejects_missing_group(runner, tmp_path) -> None:
    body = tmp_path / "feed.json"
    body.write_text(json.dumps(_verified_payload()))
    result = runner.invoke(app, [
        "evaluate-verified", "--body-file", str(body), "--group-id", "missing",
        "--quantity", "1",
    ])
    assert result.exit_code == 2
    assert "candidate-not-found" in result.stderr
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/cli/test_arbitrage_cli.py \
  tests/m1-perception/test_makefile_contract.py -q
```

- [ ] **Step 3: Implement CLI and Make target**

CLI reads a bounded local response file, parses it with
`parse_verified_feed`, evaluates, and JSON-serializes Decimal values as strings.

```make
## eval-arb-live: Paper-evaluate one verified production candidate; read-only, no orders.
eval-arb-live:
	@test -n "$(group_id)" -a -n "$(quantity)" || { \
	  echo "usage: make eval-arb-live group_id=<id> quantity=<shares>"; exit 2; }
	@BODY="$$(mktemp)"; trap 'rm -f "$$BODY"' EXIT; \
	curl -fsS "https://polyarb-l1.fly.dev/arbitrage/opportunities?min_edge_bps=0&limit=100" \
	  -o "$$BODY"; \
	uv run python -m polyarb.cli_arbitrage evaluate-verified \
	  --body-file "$$BODY" --group-id "$(group_id)" --quantity "$(quantity)" \
	  --fee-bps "$(or $(fee_bps),50)" \
	  --slippage-buffer-bps "$(or $(slippage_buffer_bps),25)"
```

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/cli/test_arbitrage_cli.py \
  tests/m1-perception/test_makefile_contract.py -q
uv run ruff check src/polyarb/cli_arbitrage.py
git add src/polyarb/cli_arbitrage.py Makefile tests/cli/test_arbitrage_cli.py \
  tests/m1-perception/test_makefile_contract.py
git commit -m "feat(m2): paper-evaluate live verified candidates"
```

### Task 4: Expose the same facts in Dashboard

**Files:**
- Create: `dashboard/lib/perception-api.ts`
- Create: `dashboard/app/events/[event_id]/page.tsx`
- Create: `dashboard/app/markets/[market_id]/page.tsx`
- Create: `dashboard/app/opportunities/page.tsx`
- Modify: `dashboard/app/layout.tsx`
- Modify: `dashboard/app/candidates/page.tsx`
- Modify: `Makefile`
- Create: `tests/m1-perception/test_dashboard_perception_contract.py`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Server-only functions `getMarketBrief`, `getEventView`, `getMarketView`, `getOpportunities`.
- All use `cache: "no-store"` and a 10-second abort signal.
- Pages render source identity, timestamps, quality, and rejection reasons.

- [ ] **Step 1: Write a failing Dashboard contract test**

```python
def test_dashboard_exposes_verified_perception_without_execution_controls() -> None:
    root = Path("dashboard")
    assert (root / "app/opportunities/page.tsx").exists()
    assert (root / "app/events/[event_id]/page.tsx").exists()
    assert (root / "app/markets/[market_id]/page.tsx").exists()
    source = "\n".join(
        path.read_text()
        for path in (
            root / "app/opportunities/page.tsx",
            root / "lib/perception-api.ts",
        )
    )
    assert "paper input only" in source.lower()
    assert "membership_hash" in source
    assert "quote_run_id" in source
    assert "placeOrder" not in source
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_dashboard_perception_contract.py -q
```

Expected: the new perception routes do not exist.

- [ ] **Step 3: Create strict TypeScript response validators**

```typescript
const L1_URL = process.env.POLYARB_L1_PUBLIC_URL ?? "https://polyarb-l1.fly.dev";

async function getJson(path: string): Promise<unknown> {
  const response = await fetch(`${L1_URL}${path}`, {
    cache: "no-store",
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`M1 HTTP ${response.status}`);
  return response.json();
}

export async function getEventView(eventId: string): Promise<EventView> {
  const value = await getJson(`/perception/events/${encodeURIComponent(eventId)}`);
  if (!isEventView(value)) throw new Error("invalid M1 event contract");
  return value;
}
```

Validators require identity, timestamps, quality, reason, and arrays; they do
not accept unknown coverage labels.

- [ ] **Step 4: Build event and market pages**

Event page renders:

```tsx
<h1>{event.title}</h1>
<p>quality: <code>{event.quality}</code> · membership: <code>{event.membership_hash}</code></p>
{event.reason && <aside role="alert">{event.reason}</aside>}
<table>{event.members.map(member => (
  <tr key={member.market_id}>
    <td>{member.title}</td><td>{member.kind}</td><td>{member.active ? "active" : "inactive"}</td>
    <td><a href={`/markets/${member.market_id}`}>inspect</a></td>
  </tr>
))}</table>
```

Market page renders current bid/ask/size, quote run, freshness, five history
points, event link, and quality. Both pages catch fetch errors and render a
visible unavailable banner, never a fabricated empty market.

- [ ] **Step 5: Build opportunities page**

Render verified candidates separately from rejection summary. Every candidate
shows event/group, legs, sum asks, gross edge, executable quantity, membership
hash, quote run, and an explicit “paper input only” banner.

- [ ] **Step 6: Add navigation and candidate drill-down**

Add `/opportunities` to layout navigation. If an L2 candidate has `event_id` or
`market_id`, link to the corresponding M1 page without removing existing TOB,
trades, or L3 links.

- [ ] **Step 7: Verify Dashboard and commit**

```bash
uv run pytest tests/m1-perception/test_dashboard_perception_contract.py -q
cd dashboard && pnpm typecheck && pnpm build
cd ..
make docs-m1-check
git add dashboard Makefile tests/m1-perception/test_m1_manual_contract.py
git commit -m "feat(dashboard): show verified market perception"
```

Expected: typecheck and production build exit 0.

### Task 5: Persist incident reasons and generate diagnostic-window reports

**Files:**
- Modify: `scripts/polywatch/healthz_watcher.py`
- Create: `src/polyarb/observation/l3_diagnostic_report.py`
- Modify: `Makefile`
- Test: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Create: `tests/m1-perception/test_l3_diagnostic_report.py`

**Interfaces:**
- Polywatch state adds `active_reasons`, `last_incident`, and `incident_count`.
- Produces `make l3-diagnostic-report manifest=<path> output=<path>`.
- Diagnostic report never emits PASS/NOT-CLOSED certification.

- [ ] **Step 1: Write failing incident-state tests**

```python
def test_recovery_retains_last_incident_reason() -> None:
    state = updated_notification_state(
        ("l2",), {}, active_reasons={"l2": "membership convergence failed"},
        notification="alert", now_s=1000, delivery_ok=True,
    )
    recovered = updated_notification_state(
        (), state, active_reasons={}, notification="recovery",
        now_s=1010, delivery_ok=True,
    )
    assert recovered["active_keys"] == []
    assert recovered["last_incident"] == {
        "keys": ["l2"],
        "reasons": {"l2": "membership convergence failed"},
        "started_at_s": 1000,
        "recovered_at_s": 1010,
    }
    assert recovered["incident_count"] == 1
```

- [ ] **Step 2: Write failing diagnostic report test**

```python
def test_diagnostic_report_keeps_all_anomalies_without_certifying(tmp_path) -> None:
    rows = [
        _event("subscription_control_failed", "evidence_timeout", at="2026-07-26T09:28:32Z"),
        _event("subscription_control_failed", "evidence_timeout", at="2026-07-26T10:36:26Z"),
    ]
    report = build_diagnostic_report(_manifest(), rows, end="2026-07-27T08:51:13Z")
    assert report["mode"] == "diagnostic-window"
    assert report["anomaly_counts"] == {"subscription_control_failed": 2}
    assert report["incidents"][0]["first_at"] == "2026-07-26T09:28:32Z"
    assert "verdict" not in report
```

- [ ] **Step 3: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_polywatch_healthz_watcher.py \
  tests/m1-perception/test_l3_diagnostic_report.py -q
```

- [ ] **Step 4: Implement durable incident context**

Pass a `{component_key: reason}` mapping into state updates. On first alert,
persist start time/reasons. On recovery, preserve one bounded last incident.
Cap reason strings at 200 characters and store no URLs, response bodies, or
secrets.

- [ ] **Step 5: Implement read-only anomaly aggregation**

The report loads the immutable manifest identity and derives `end` as
`min(current UTC time, manifest end)`. It queries runtime events inside
`[T0, end)`, groups by `(kind, reason_code)`, records first/last/count, and
includes health sample counts/gaps. Write output with O_EXCL. The top-level keys
are:

```python
{
    "mode": "diagnostic-window",
    "manifest_hash": manifest_hash,
    "start": start,
    "end": end,
    "sample_summary": sample_summary,
    "anomaly_counts": anomaly_counts,
    "incidents": incidents,
}
```

Do not import or reuse strict verdict labels.

- [ ] **Step 6: Add Make target, run tests, and commit**

```make
## l3-diagnostic-report: Aggregate one immutable diagnostic interval; never certifies PASS.
l3-diagnostic-report:
	@test -n "$(manifest)" -a -n "$(output)" || { \
	  echo "usage: make l3-diagnostic-report manifest=<path> output=<new-path>"; exit 2; }
	@uv run python -m polyarb.observation.l3_diagnostic_report \
	  --manifest "$(manifest)" --output "$(output)"
```

```bash
uv run pytest tests/m1-perception/test_polywatch_healthz_watcher.py \
  tests/m1-perception/test_l3_diagnostic_report.py -q
uv run ruff check scripts/polywatch/healthz_watcher.py \
  src/polyarb/observation/l3_diagnostic_report.py
git add scripts/polywatch/healthz_watcher.py \
  src/polyarb/observation/l3_diagnostic_report.py Makefile \
  tests/m1-perception/test_polywatch_healthz_watcher.py \
  tests/m1-perception/test_l3_diagnostic_report.py
git commit -m "feat(ops): retain incident reasons and diagnostic evidence"
```

### Task 6: End-to-end qualification, teaching, and production handoff

**Files:**
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/27-M1如何指导M2而不越权.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: active M1 phase evidence log

**Interfaces:**
- Defines one supported path: brief → event → market → verified opportunity → M2 paper evaluation.
- Keeps real execution unavailable.

- [ ] **Step 1: Add the exact practical workflow**

Document:

```bash
make market-brief-prod
make show-event-prod event_id=<event_id>
make show-market-prod market_id=<market_id>
make scan-arb-live min_edge_bps=0
make eval-arb-live group_id=<group_id> quantity=<shares>
```

For every command explain: what fact it answers, which timestamp/identity to
check, what failure means, and why it does not authorize an order.

- [ ] **Step 2: Run full local verification**

```bash
uv run pytest -q
uv run ruff check src/polyarb/routing/verified_candidate.py \
  src/polyarb/routing/neg_risk_paper.py src/polyarb/cli_arbitrage.py \
  scripts/polywatch/healthz_watcher.py \
  src/polyarb/observation/l3_diagnostic_report.py
cd dashboard && pnpm typecheck && pnpm build
cd ..
make docs-m1-check
make planning-status
```

Expected: full pytest passes with only established documented skip/xfail,
changed-file Ruff exits 0, Dashboard gates exit 0, and no planning drift.

- [ ] **Step 3: Deploy isolated surfaces**

Record exact current L1/L2/Dashboard identities. Deploy L1 for API/CLI contract,
the Fly cron image for Polywatch state, and Dashboard for pages. Do not restart
L2 or reset its diagnostic interval merely to deploy unrelated surfaces.

- [ ] **Step 4: Verify production workflow**

```bash
make market-brief-prod
make show-event-prod event_id=111080
make diagnose-arb-feed-prod min_edge_bps=0
make polywatch-healthz-dry
make polywatch-resident-status
```

Select one actual `complete-supported` group from the feed and run:

```bash
make show-event-prod event_id=<verified_event_id>
make show-market-prod market_id=<verified_market_id>
make eval-arb-live group_id=<verified_group_id> quantity=1
```

Expected:

- Michigan augmented event is explained and rejected;
- selected standard event membership matches every candidate leg;
- paper evaluator prints only BUY YES legs and separate gross/cost/net values;
- no execution result, order ID, wallet action, or position mutation appears;
- resident state includes last incident context after a controlled fixture-level
  state transition; no production fault injection is required.

- [ ] **Step 5: Authenticated browser UAT**

In the persistent authenticated Edge instance open:

```text
/status
/events/111080
/markets/969762
/opportunities
```

Verify real application content, source timestamps, identities, rejection
reason, and links. A login page or route HTTP 200 alone is not a PASS.

- [ ] **Step 6: Close documentation and commit**

Record exact responses, screenshot references, release identities, diagnostic
window outcome, known limitations, and the next M2 strategy action in the
phase evidence log, JOURNAL, STATE, manual, and learning index. Then:

```bash
make planning-status
git add docs .planning
git commit -m "docs(m1): qualify market-to-M2 practical workflow"
git push
```
