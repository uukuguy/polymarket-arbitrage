# Phase 1 Security Review

**Audited:** 2026-04-29
**Scope:** Plan-level (5 PLAN.md files), pre-execution
**ASVS Level:** L1
**Verdict:** REVISE

## Summary

The plan is in good shape for a local, single-developer tool: stdlib `sqlite3` with parameterized `executemany`, `yaml.safe_load`, atomic `os.replace` Parquet writes, no auth flows, no public-facing surface. Three categories deserve hardening before execution: (1) **HTTP-redirect / response-size bounds on the Polymarket trust boundary** (Gamma is external untrusted JSON; the plan has no max-page guard, no redirect cap, no response-size cap, and the orchestrator silently calls `float(asks[0]["price"])` and `float(asks[0]["size"])` on attacker-controlled fields without try/except — a single malformed string crashes the whole snapshot); (2) **`db_path` / `parquet_root` from YAML are not constrained under the project root** (a user-edited config or env var like `POLYARB_DB_PATH=/etc/passwd` would be opened); (3) **the recorded fixtures task does not require sanitization** before the executor commits real network captures into git. None are critical for a local tool, but each is a 1-line plan edit and worth fixing now before code locks them in.

Top-3 priorities: F-1 (orchestrator price/size coercion crashes), F-2 (httpx redirect + response-size cap), F-3 (Settings path validators).

## Threat Findings

### F-1: Orchestrator coerces attacker-controlled price/size with bare `float()`
- **Severity:** HIGH
- **Category:** T4 API
- **Plan:** 01-4-PLAN.md, task T2 (orchestrator step 5)
- **Evidence:** Plan T2 Step 5 hardcodes:
  ```python
  if asks:
      m["best_ask_price"] = float(asks[0]["price"])
      m["best_ask_size"]  = float(asks[0]["size"])
  if bids:
      m["best_bid_price"] = float(bids[0]["price"])
      m["best_bid_size"]  = float(bids[0]["size"])
  ```
  No try/except, no schema check. This is inside the main pipeline, not the validator. Adjacent code in `validator/layers.py` (Plan 3 T5) does protect with `float(asks[0]["price"]) if asks else None` but still does not catch `ValueError` if the price is a non-numeric string.
- **Risk:** A single malformed CLOB book record (e.g., `{"asks": [{"price": "NaN-foo", "size": null}]}`, or `asks[0]` missing the `price` key) raises `ValueError` / `KeyError` / `TypeError` and aborts the entire snapshot **before** Plan 3's transactional write — meaning no SQLite row, no validation issue logged, no Parquet file. The CLOB API is external untrusted input. Even without malice, partial / truncated responses will hit this. This is the project's own "fail-secure but persistent" goal (D-D3) that the plan otherwise honors carefully.
- **Mitigation:** In Plan 4 T2 step 5, wrap each top-of-book extraction in a try/except that catches `(KeyError, TypeError, ValueError, IndexError)`, logs a warning, and appends an `Issue(layer=4, category=Category.UNKNOWN, market_id=m["market_id"], detail=f"unparseable book for {tid}: {e}", raw_payload=json.dumps(book, default=str)[:500])`. Same pattern in Plan 3 T5 `layer4_cross_source` for the `float(asks[0]["price"])` lines and the `float(ref)` line. Truncate `raw_payload` to ~500 bytes to prevent huge payloads filling the issues table.
- **Effort:** S (line edit in two plan tasks)

### F-2: httpx client follows redirects and has no response-size cap
- **Severity:** MEDIUM
- **Category:** T4 API
- **Plan:** 01-2-PLAN.md, task T2 (GammaClient)
- **Evidence:** Plan T2 spec:
  ```python
  self._http = httpx.AsyncClient(timeout=settings.http_timeout_s,
      limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
      headers={"User-Agent": "polyarb/0.1"}, http2=True)
  ```
  Plan does not set `follow_redirects=False`, no `max_redirects` cap, and `fetch_all_active_markets` keeps paginating until a short page arrives (no `MAX_PAGES` guard). Plan also does not bound `r.json()` size.
- **Risk:** Three concrete issues. (1) httpx **default is `follow_redirects=False`** in v0.27, so this one is actually OK by default — but the plan should pin it explicitly because a future httpx default-flip would silently expose redirect-based SSRF (cloud metadata, file://, etc.) — Polymarket's CDN could redirect anywhere on a misconfiguration. (2) A buggy or hostile Gamma endpoint returning `[full_page_of_100_markets]` forever would loop until OOM — the plan has no `MAX_PAGES` ceiling. (3) A single page returning a 1GB JSON body would be parsed entirely into memory; httpx has no per-response size cap and `r.json()` reads everything.
- **Mitigation:** In Plan 2 T2 (`GammaClient.__init__`), explicitly add `follow_redirects=False` to the `AsyncClient` constructor. Add a class constant `MAX_PAGES = 1000` (300k markets is far above any realistic Polymarket size — it's currently ~20k active) and break out of the pagination loop if `offset // PAGE_LIMIT >= MAX_PAGES`, logging an issue. Optionally add `r.headers.get("content-length")` check and skip + log if it exceeds, say, 100MB.
- **Effort:** S (3-line plan addition)

### F-3: Settings path fields accept arbitrary absolute paths from YAML / env
- **Severity:** MEDIUM
- **Category:** T2 Path / T3 YAML
- **Plan:** 01-1-PLAN.md, task T3 (Settings)
- **Evidence:** Plan T3 declares:
  ```python
  db_path: Path = Path("data/state.db")
  parquet_root: Path = Path("data/snapshots")
  ```
  No `field_validator`. `model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")` means `POLYARB_DB_PATH=/etc/passwd` or a user-edited `config/snapshot.yaml` with `db_path: /tmp/x` is silently honored. SQLiteStore.__init__ then runs `self._db_path.parent.mkdir(parents=True, exist_ok=True)` and `sqlite3.connect(self._db_path)` on whatever path arrives.
- **Risk:** This is a local dev tool so the threat model is mild — but it's a footgun. A misconfigured CI shell with `POLYARB_PARQUET_ROOT=/` would have `os.replace(tmp, "/2026/04/29/12-00-00.parquet")` attempt to write at root. A typo like `db_path: ../../shared/state.db` writes outside the project. More subtly, since `data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet` includes the timestamp from `datetime.now()` (T-3 of Plan 3 step 5: `compute_snapshot_path`), the path is safe **only** if `parquet_root` itself is constrained.
- **Mitigation:** In Plan 1 T3, add a `@field_validator("db_path", "parquet_root")` to Settings that resolves the path and asserts it is relative to the project root (or an explicitly-allowed list). Pseudocode:
  ```python
  @field_validator("db_path", "parquet_root")
  @classmethod
  def _within_project(cls, v: Path) -> Path:
      project_root = Path.cwd().resolve()  # or pass via env
      resolved = (project_root / v).resolve() if not v.is_absolute() else v.resolve()
      try:
          resolved.relative_to(project_root)
      except ValueError:
          raise ValueError(f"path {v} resolves outside project root {project_root}")
      return resolved
  ```
  Alternative (simpler) mitigation: just document that absolute paths are rejected and add the validator. Update Plan 1 T6's tests to cover this.
- **Effort:** S (one validator + one test)

### F-4: Recorded fixtures task does not require sanitization or commit-policy decision
- **Severity:** MEDIUM
- **Category:** T6 Fixtures
- **Plan:** 01-2-PLAN.md, task T1
- **Evidence:** T1 records live API responses to `tests/m1-perception/fixtures/gamma_sample.json` and `clob_sample.json` and uses `json.dumps(obj, indent=2, default=lambda o: o.__dict__ if hasattr(o, "__dict__") else str(o))`. The `default=lambda o: o.__dict__` is the concern: py-clob-client SDK objects (BookParams, possibly internal client wrappers) may have `__dict__` containing the configured `host`, internal session state, or — if a future SDK adds wallet support and a developer left credentials in env — references to keys/headers. The plan also does **not specify** whether fixtures are committed to git (the .gitignore in Plan 1 T5 does NOT exclude `tests/**/fixtures/`, so by default they will be committed).
- **Risk:** First commit of fixtures could leak: (a) any `Authorization`, `x-api-key`, or `cookie` headers if the recording script accidentally sets them; (b) Cloudflare ray IDs / org-correlated headers; (c) if py-clob-client is later upgraded to L1+ and a developer re-records fixtures, **wallet private key references** could land in `__dict__` dumps. Even on L0, the `host` URL is fine but the `default=str` fallback could serialize unexpected objects into JSON-string form that's hard to audit.
- **Mitigation:** In Plan 2 T1, add explicit instructions: (1) After writing fixtures, run a sanitization pass: `jq 'walk(if type=="object" then with_entries(select(.key | test("auth|key|cookie|token|secret"; "i") | not)) else . end)' < raw.json > clean.json` (or a Python equivalent). (2) Restrict `default=` to a whitelist: `default=lambda o: {"token_id": getattr(o, "token_id", None), "side": getattr(o, "side", None)} if isinstance(o, BookParams) else str(o)`. (3) Plan should explicitly state "fixtures ARE committed to git; before commit, manually inspect for any `Authorization`/`Cookie`/`Set-Cookie` headers and remove them". (4) Add a CI-friendly check or a pytest assertion that fixture files contain none of those substrings.
- **Effort:** M (writing recording-script guidance + test)

### F-5: `record_issues` writes attacker-controlled `raw_payload` and `detail` strings unbounded
- **Severity:** LOW
- **Category:** T1 SQLi (informational — the SQL itself is parameterized correctly)
- **Plan:** 01-3-PLAN.md, task T2 (SQLiteStore.write_snapshot) and T5 (layer2_fields)
- **Evidence:** Plan T2 builds `(snapshot_id, issue.layer, issue.category.value, issue.market_id, issue.detail, issue.raw_payload)` and passes via `executemany("INSERT INTO validation_issues(...) VALUES (?,?,?,?,?,?)", issue_tuples)`. Parameterization is correct — no SQLi exposure. **However**, `issue.raw_payload` in Plan T5 is built via `json.dumps({k: m.get(k) for k in REQUIRED_FIELDS}, default=str)` with no size cap. A market dict with a 10MB `question` field would inflate the issues table without bound. Same in Plan 4 T2 where `Issue(... detail=f"Gamma unreachable: {e}")` captures the full exception message which could be a 4xx response body.
- **Risk:** Gradual disk fill if a misbehaving market has gigantic fields. Cosmetic, not an injection vector. Worth flagging because the plan says "validation_issues stores raw_payload" without saying "but cap it".
- **Mitigation:** In Plan 3 T5, change `raw_payload=json.dumps(...)` to `raw_payload=json.dumps(...)[:1024]` (cap at 1KB). In Plan 4 T2 step 1 + step 4, change `detail=f"Gamma unreachable: {e}"` to `detail=f"Gamma unreachable: {str(e)[:200]}"`. Document the cap in CONTEXT.md.
- **Effort:** S (per-line)

### F-6: Plan does not specify malformed-JSON handling at the httpx boundary
- **Severity:** LOW
- **Category:** T4 API
- **Plan:** 01-2-PLAN.md, task T2 (GammaClient._get)
- **Evidence:** T2 `_get` body: `r.raise_for_status(); return r.json()`. If the server returns 200 with a non-JSON or truncated body, `r.json()` raises `json.JSONDecodeError` which is **not** in `retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException))`. The plan declares `RetryError on exhaustion` for transient errors but is silent on JSON-parse failures — they would propagate raw out of `fetch_all_active_markets`, which the orchestrator catches as a generic `Exception` and converts to `API_UNREACHABLE`. That's acceptable but the plan should say so.
- **Risk:** A truncated mid-flight Gamma response on a flaky network would surface as "API_UNREACHABLE" rather than as a more specific category — fine, but inconsistent with the explicit categorization the project values. The handling is also undocumented, which is itself a finding (Plan should say "JSON parse errors are non-retryable and surface as API_UNREACHABLE via orchestrator catch-all").
- **Mitigation:** In Plan 2 T2, add to the action: "JSON parse errors (`json.JSONDecodeError`) are NOT retried — they surface to the orchestrator as a non-retry exception. Document in module docstring." Optionally add a `Category.MALFORMED_RESPONSE` to the enum in Plan 3 T4 and have orchestrator distinguish.
- **Effort:** S (doc edit) or M (new category + handling)

### F-7: py-clob-client is not hash-pinned
- **Severity:** LOW
- **Category:** T5 SDK
- **Plan:** 01-1-PLAN.md, task T1
- **Evidence:** T1 declares `py-clob-client>=0.34.6,<0.35` in pyproject.toml runtime deps. No lockfile (uv.lock, poetry.lock, requirements.txt with `--hash=sha256:...`) is mandated. `pip install -e '.[dev]'` will fetch whatever PyPI serves at install time within that range. The SDK is a third-party that itself reaches out to clob.polymarket.com; supply-chain compromise would silently route data through the attacker's library version.
- **Risk:** Standard Python supply-chain risk. Low for a local dev tool with infrequent reinstalls; higher when CI runs in untrusted runners. The plan does not mandate `pip install --require-hashes` or equivalent, nor does it specify whether to commit a lockfile.
- **Mitigation:** In Plan 1 T1, add a sub-task: "After `pip install -e '.[dev]'` succeeds in T7, run `pip freeze > requirements.lock` and commit it. CI uses `pip install -r requirements.lock` instead of resolving from pyproject ranges." Or, preferred for Python 3.12 era: switch the project to `uv` and commit `uv.lock`. CLAUDE.md "Code Style Preferences" already mandates uv elsewhere — this would align.
- **Effort:** M (add task + lockfile commit)

### F-8: Default datetime parsing in normalizer accepts naive timestamps as UTC silently
- **Severity:** INFO
- **Category:** T4 API
- **Plan:** 01-4-PLAN.md, task T1 (normalizer)
- **Evidence:** Plan T1: `dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00")); if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)`. Treating a naive timestamp as UTC is a defensible default but unaudited — a Gamma response with a local-time `endDate` would be off by hours.
- **Risk:** Behavioral / correctness, not security. Worth a one-line comment.
- **Mitigation:** Plan T1 already documents in the test (test 9 in Plan 5 T2 — `test_normalize_endDate_naive_iso`). No change needed; flagged for completeness.
- **Effort:** S (no-op or comment)

## What's Done Right

- **Parameterized SQL throughout.** Plan 3 T1's `MARKETS_INSERT_SQL` is a static string with `?` placeholders; T2's `executemany` and `validation_issues` insert both use parameter binding. The DDL is module-level constant. Token IDs flow as `?`-bound parameters end-to-end and are typed as TEXT — no f-string SQL anywhere in the plans, no dynamic table/column names from API data. **T1 SQLi posture is solid.**
- **`yaml.safe_load` mandated, not `yaml.load`.** Plan 1 T3 says `yaml.safe_load` explicitly; Plan 1 T6's test verifies the load path. Combined with `extra="ignore"` on Settings, an attacker-edited YAML cannot inject arbitrary fields into Settings, and arbitrary Python object construction via `!!python/object` is blocked.
- **No private keys, no signing, no auth flow.** The plan explicitly rejects wallet/key loading (Plan 2 T3 spec: "DO NOT add wallet/key loading — read-only L0 endpoint"). This is the right call and removes an entire class of credential-leak findings.
- **Atomic writes with same-filesystem tmp + os.replace.** Plan 3 T3's spec uses `out_path.with_suffix(out_path.suffix + ".tmp")` so the tmp file lives under `data/snapshots/YYYY/MM/DD/` — same filesystem as the final, so `os.replace` is atomic, no cross-mount rename trap.
- **uint256 token IDs preserved as strings end-to-end.** Plan 3 T1 mandates `pa.string()` in pyarrow schema; Plan 4 T1 normalizer keeps `str(token_list[0])`; Plan 3 T6 test_token_ids_preserve_uint256_string verifies. This is both a correctness and a defense-in-depth win against any accidental int-coercion injection.
- **Path traversal for parquet output is structurally safe.** `compute_snapshot_path` derives every path component from `datetime.fromtimestamp(...).strftime("%Y") / strftime("%m") / ...`, never from external strings — no API field reaches the path constructor.

## Recommendations

1. **In Plan 4 PLAN.md, Task T2, Step 5**, wrap each `float(asks[0]["price"])` / `float(asks[0]["size"])` / `float(bids[0]["price"])` / `float(bids[0]["size"])` in a try/except `(KeyError, TypeError, ValueError, IndexError)` that logs a warning, appends an `Issue(layer=4, category=Category.UNKNOWN, market_id=m["market_id"], detail=f"unparseable book: {e}", raw_payload=json.dumps(book, default=str)[:500])`, and continues. Mirror the same pattern in Plan 3 T5 `layer4_cross_source` for the `float(asks[0]["price"])` and `float(ref)` calls. (F-1)

2. **In Plan 2 PLAN.md, Task T2 (`GammaClient.__init__`)**, add `follow_redirects=False` explicitly to the `httpx.AsyncClient` constructor and add class constant `MAX_PAGES = 1000` to `GammaClient`. In `fetch_all_active_markets`, raise `RuntimeError(f"pagination exceeded {self.MAX_PAGES}")` if `offset // PAGE_LIMIT >= MAX_PAGES`. (F-2)

3. **In Plan 1 PLAN.md, Task T3 (Settings)**, add a `@field_validator("db_path", "parquet_root")` that resolves the path and asserts it is under the project root (or current working directory). Add a Plan 1 T6 test `test_db_path_outside_project_rejected` to verify. (F-3)

4. **In Plan 2 PLAN.md, Task T1**, after the recording step, add an explicit sanitization sub-step: replace `default=lambda o: o.__dict__ ...` with a whitelist that only extracts known-safe attributes from BookParams (`token_id`, `side`). Add a sentence: "Fixtures are committed to git; before committing, grep for `authorization`, `cookie`, `x-api-key`, `bearer` (case-insensitive) and remove if found." Add a regression test `test_fixtures_have_no_credentials` that scans both fixture files. (F-4)

5. **In Plan 3 PLAN.md, Task T5 (layer2_fields)**, change the `raw_payload=json.dumps(...)` line to `raw_payload=json.dumps(...)[:1024]`. **In Plan 4 PLAN.md, Task T2**, truncate exception messages: `detail=f"Gamma unreachable: {str(e)[:200]}"` and `detail=f"CLOB unreachable: {str(e)[:200]}"`. (F-5)

6. **In Plan 2 PLAN.md, Task T2**, document in the module docstring: "JSON parse errors at the httpx boundary (`json.JSONDecodeError`) are NOT included in tenacity's `retry_if_exception_type` — they propagate immediately and the orchestrator categorizes them as `API_UNREACHABLE`. This is intentional: a 200 with malformed JSON usually means a CDN/cache misconfiguration, not a transient network issue." (F-6)

7. **In Plan 1 PLAN.md, Task T7**, after `pip install -e '.[dev]'` succeeds, run `pip freeze --exclude-editable > requirements.lock` and commit it. Update `.gitignore` in Plan 1 T5 to NOT exclude `requirements.lock`. Add a follow-up note in 01-1-SUMMARY.md to track migration to `uv lock` per CLAUDE.md preference. (F-7)

## Out of Scope (intentionally not flagged)

- **Authentication / authorization / session management** — none required; this is a single-user local dev tool that only reads public Polymarket endpoints. No T1 (auth) findings to report.
- **TLS configuration** — already given via httpx defaults; the audit was instructed to skip.
- **Logging of sensitive data** — loguru is configured with structured fields; no PII or credentials are processed by the system, so log injection / log-leak surface is minimal. The only attacker-controlled string that reaches logs is the Gamma error message, which is bounded by F-5's truncation recommendation.
- **Cross-site / browser security** (CORS, CSP, cookies) — no web frontend exists in Phase 1.
- **Dependency CVE scanning** — out of plan-level audit scope; `pip-audit` should run in CI, but that's a Phase 5 (industrialize) concern.

## SECURITY REVIEW COMPLETE

**Verdict: REVISE.** Found **0 CRITICAL, 1 HIGH, 3 MEDIUM, 3 LOW, 1 INFO**. The single HIGH (F-1, orchestrator's bare `float()` on attacker-controlled CLOB book fields) is a pre-execution paper-cut that contradicts the plan's own "fail-secure but persistent" goal — fix before code locks it in. The MEDIUMs (F-2 redirect/page-cap, F-3 Settings path validators, F-4 fixture sanitization) are each one-line plan edits. The plan's overall security posture is strong: parameterized SQL, `yaml.safe_load`, no auth/keys, atomic writes, and string-typed uint256 IDs end-to-end. After applying the 7 recommendations above, this plan is ASVS L1 ready for execution.

---

## Resolution Status (applied 2026-04-29)

| ID | Severity | Status | Where applied |
|---|---|---|---|
| F-1 | HIGH | ✅ APPLIED | 01-4-PLAN.md T2 step 5 (try/except wrapping ask/bid float coercion → Issue(UNKNOWN)); 01-3-PLAN.md T5 layer4 (`_safe_float` helper + skip-on-unparseable) |
| F-2 | MEDIUM | ✅ APPLIED | 01-2-PLAN.md T2 (`follow_redirects=False` explicit + `MAX_PAGES=1000` ceiling with RuntimeError on exceed) |
| F-3 | MEDIUM | ✅ APPLIED | 01-1-PLAN.md T3 (`@field_validator` on `db_path`/`parquet_root` rejecting out-of-project paths; `POLYARB_ALLOW_EXTERNAL_PATHS=1` test escape hatch); 01-5-PLAN.md T1 conftest (sets the env var at module top) |
| F-4 | MEDIUM | ✅ APPLIED | 01-2-PLAN.md T1 (whitelist `_safe_default` replaces `o.__dict__`; credential-leak grep mandatory before completing); 01-5-PLAN.md T1 conftest (regex-based credential scanner runs at import) |
| F-5 | LOW | ✅ APPLIED | 01-3-PLAN.md T5 layer2 (`raw_payload[:1024]`, `detail[:200]`); 01-4-PLAN.md T2 (Gamma + CLOB exception detail capped at 200 chars; ghost-book raw_payload at 500) |
| F-6 | LOW | 📝 DOCUMENTED | 01-2-PLAN.md T2 module docstring instruction (json.JSONDecodeError is non-retryable, propagates to orchestrator API_UNREACHABLE). No new Category enum value added — Phase 3 may revisit if signal-to-noise warrants it |
| F-7 | LOW | ⏸️ DEFERRED | Lockfile (uv.lock or requirements.lock) deferred to post-Phase-1. Rationale: project is single-developer local; supply-chain risk is low for now. **Trigger to revisit:** when CI is added (m5-industrialize) OR before any real-money trading (any phase that introduces wallet/auth flows). Track in `threads/learnings-meta.md` SESSION 05 |
| F-8 | INFO | ✅ ALREADY OK | 01-5-PLAN.md T2 test 9 (`test_normalize_endDate_naive_iso`) already documents the naive→UTC behavior. No change needed |

**Net change:** 7 plan edits across 4 PLAN.md files (01-1, 01-2, 01-3, 01-4) plus 1 conftest update in 01-5. No structural changes — all surgical inline edits to specific tasks. Wave assignments and dependency graph unchanged. Plan-checker verdict (PASS, 12/12 dimensions) remains valid since no new tasks were introduced.

**Updated verdict: PASS for execution.** Ready for `/gsd-execute-phase 1 --ws m1-perception`.

