# M1 Market Perception Platform Living Manual Design

**Date:** 2026-07-18  
**Workstream:** `m1-perception`  
**Target:** product-and-operations documentation for the complete M1 capability line  
**Status:** user-approved design, awaiting written-spec review

## 1. Problem

M1 has accumulated working collection, health, observation, dashboard, and
production-operation capabilities across source code, Make targets, phase
artifacts, learning notes, and runbooks. Those materials are useful to a
developer working on one subsystem, but they do not currently answer the
operator's end-to-end questions from one stable entry point:

- What can the M1 platform do today?
- Which parts are verified for practical use, conditional, or unavailable?
- How do I inspect production, run locally, and use the dashboard?
- What does each health or data signal mean?
- Which outputs support a market decision, and which do not authorize trading?
- How does the guide stay synchronized as implementation changes?

The current documentation is fragmented by purpose. `.planning/CURRENT.md` is
the source for volatile operational truth, `docs/learning/` teaches concepts,
phase artifacts preserve implementation history, and the Makefile exposes
commands. The README does not provide a reliable product-level M1 entry point
and can retain obsolete production status. Copying all of these facts into a
new static document would create another source of drift.

## 2. Goals and non-goals

### Goals

- Create one Chinese, product-and-operations manual for M1.
- Let a capable operator complete a five-minute daily assessment without
  reading phase internals.
- Describe the complete collection-to-perception chain and its current usable
  boundary.
- Distinguish local state, deployed production state, and historical evidence.
- Give every capability an explicit readiness classification and verification
  path.
- Add an offline contract check and a narrowly scoped maintenance gate so the
  manual evolves with user-visible M1 surfaces.
- Link the manual from the repository's natural entry points.

### Non-goals

- Replace `.planning/CURRENT.md` as the source of volatile production truth.
- Replace learning documents, design threads, or phase evidence.
- Embed live network checks in documentation CI or pre-commit.
- Declare Phase 05, L3 evidence, strategy profitability, or real-money trading
  ready merely because the manual exists.
- Document every internal class, field, log message, or refactor.
- Turn the manual into a second roadmap or an exhaustive API reference.

## 3. Chosen approach: hybrid living manual

Three approaches were considered:

1. A hand-maintained single document is readable but quickly duplicates and
   drifts from operational truth.
2. A fully generated document is fresh for mechanically discoverable facts but
   weak at explaining mental models, limitations, and decision boundaries.
3. A hybrid living manual keeps human-authored guidance while automatically
   checking stable references and delegating volatile facts to their canonical
   sources.

The third approach is selected. The main artifact will be:

`docs/M1-市场感知平台使用手册.md`

The manual owns the narrative, workflows, interpretation, safety boundaries,
and readiness classifications. It references rather than copies transient
production facts. An offline checker validates mechanically discoverable
contracts such as Make targets, local files, routes, health-check names,
required sections, metadata, and readiness labels.

## 4. Truth ownership

The documentation must make its truth hierarchy explicit:

| Fact type | Canonical source | Manual behavior |
|---|---|---|
| Current production state, ages, counts, active incident | `.planning/CURRENT.md`, production L1/L2 `/health` | Explain how to query and interpret; do not copy ephemeral values as evergreen prose |
| Stable operator commands | `Makefile` and CLI implementation | List by workflow; offline checker verifies referenced targets exist |
| Dashboard and HTTP surfaces | route implementation | Explain purpose and access; offline checker verifies named routes |
| Health contract and check names | health implementation | Explain meaning and response; offline checker verifies referenced checks |
| Storage and data contracts | migrations, schemas, implementation | Summarize operator-facing meaning and link deeper references |
| Design rationale and concepts | `docs/learning/`, `.planning/threads/`, phase artifacts | Link selectively; do not duplicate their full content |
| Capability readiness | manual plus latest verification evidence | Maintain a dated classification with evidence path and known boundary |

If sources conflict, volatile deployed reality comes from the current production
health surfaces and `.planning/CURRENT.md`; stable interface truth comes from
code and the Makefile. The manual must state the discrepancy and be corrected,
not silently override either source.

## 5. Manual information architecture

The manual has ten required sections.

### 5.1 Thirty-second mental model

Present the end-to-end platform as a small chain:

`market discovery -> L1 snapshots -> candidate selection -> L2 books -> L3 observation -> operator decision`

Explain that M1 observes and validates markets. It does not by itself authorize
capital deployment or claim an executable arbitrage return.

### 5.2 Capability matrix

Every material M1 capability uses exactly one readiness label:

- `已验证可用` — its documented workflow and evidence have passed the stated
  verification in the relevant environment;
- `有条件可用` — the function works under named prerequisites or has an
  unresolved operational/evidence limitation;
- `尚不可用` — not implemented, not verified, or explicitly outside the
  current usable boundary.

Each matrix row must include purpose, data source, verification path, known
limitations, and prohibited use. A `last verified` date may be included, but
live ages and counts must be queried rather than frozen in the table.

### 5.3 Five-minute daily check

Give the shortest safe sequence for answering:

1. Is production reachable?
2. Is L1 collection fresh?
3. Is the L1-to-L2 cursor chain converged?
4. Are candidate, WebSocket, and mirror data fresh or truthfully quiet?
5. Does L3 have sufficient observation evidence?
6. Is there any active opportunity, warning, or explicit stop condition?

The workflow must say what a healthy, warning, failure, or insufficient-evidence
result permits the operator to conclude.

### 5.4 Three operating workflows

Document separately:

- production inspection: read-only commands and health surfaces;
- local development/verification: environment setup, services, collection, and
  diagnostics;
- dashboard use: entry route, authentication expectations, panels, freshness,
  and common interpretation mistakes.

Local database counts must never be presented as production platform status.
Candidate and production environments must be named wherever confusion is
plausible.

### 5.5 Data and freshness contracts

Describe L1, L2, and L3 at the operator level: what each layer stores, how data
flows between layers, timestamps/freshness semantics, cursor meaning,
projection/mirror behavior, and the difference between transport liveness and
business-data freshness. Link to deeper learning or architecture documents for
implementation detail.

### 5.6 Reading results and trading boundaries

Explain what snapshots, candidates, books, observations, and opportunity output
can establish. Explicitly identify what they cannot establish: fill certainty,
fees and slippage not represented by the observation, oracle correctness,
capital/risk approval, and sustained profitability. No M1 signal alone is a
real-money execution instruction.

### 5.7 Troubleshooting map

Provide symptom-to-check-to-action guidance for at least:

- local overview reports zero data;
- L1 freshness is warning or failing;
- L2 reports `WAITING_FOR_EVENT`;
- cursor lag is positive or stale;
- mirror freshness is stale;
- L3 remains `0/10` or otherwise lacks evidence;
- opportunity count is zero;
- dashboard authentication or route access fails;
- R2 or archival status warns.

Actions should start read-only and identify when a command mutates production,
restarts a service, or invokes chaos behavior.

### 5.8 Operations and recovery

Cover normal start/stop/restart/redeploy concepts, configuration boundaries,
health verification after an action, evidence capture, and escalation paths.
The manual should link specialized runbooks instead of reproducing every
incident procedure.

### 5.9 Command index

Classify all documented commands as:

- daily safe/read-only;
- local mutation;
- production mutation;
- chaos/failure injection.

Each command needs a purpose, expected environment, prerequisites, and a short
interpretation of success. Every executable workflow must use a Makefile entry
point when the project exposes one.

### 5.10 Maintenance protocol and changelog

State which user-visible changes require a manual review, how to run the
offline contract check, who owns volatile state, and how capability labels are
updated. Keep a concise changelog for manual contract changes rather than
recording ordinary internal refactors.

## 6. Synchronization design

### 6.1 Offline contract check

A repository script, exposed as `make docs-m1-check`, will validate without
network access:

- every Make target referenced by the manual exists;
- every repository-relative link resolves;
- named dashboard/HTTP routes exist in the route contract;
- named health checks exist in the health implementation;
- all ten required sections and required metadata are present;
- capability rows use only the three defined readiness labels;
- capability entries contain their required evidence and boundary fields.

The checker should produce actionable messages naming the broken reference and
the expected source of truth. It does not assert current production freshness;
that belongs to runtime health checks.

### 6.2 Scoped pre-commit gate

The existing pre-commit flow will invoke the check for changes to the manual or
its checker. It will also require the manual to be reviewed when staged changes
alter a user-visible M1 contract, including:

- M1 Makefile command entry points;
- public CLI commands or flags used by M1 workflows;
- health-check names or operator-visible health semantics;
- operator-visible database migrations or schemas;
- dashboard or HTTP routes documented by the manual.

The implementation plan must define path and semantic triggers narrowly enough
that internal refactors, test-only changes, and log wording do not demand a
manual edit. A triggered commit may satisfy the gate either by staging a manual
update or by an explicit, auditable no-documentation-impact mechanism defined
in the implementation plan. Bypassing hooks is not the mechanism.

### 6.3 Workflow discipline

Each M1 phase or material feature handoff must review:

1. capability label and evidence path;
2. affected daily or recovery workflow;
3. commands, routes, health names, and data contracts;
4. limitations and prohibited uses;
5. learning-document links.

The manual check becomes part of relevant plan verification and phase closure.

## 7. Entry points and document relationships

Implementation will add concise links to the living manual from:

- `README.md`, as the primary product/operations entry for M1;
- `docs/learning/00-INDEX.md`, distinguishing operation from learning order;
- `.planning/CURRENT.md`, pointing operators to stable interpretation and
  workflows while CURRENT retains volatile status.

The manual will link back to these canonical sources and to focused learning,
architecture, setup, and runbook documents. It must avoid circular claims where
two documents cite each other as the authority for the same fact.

## 8. Failure and conflict behavior

- A broken target, link, route, health name, required section, or capability
  record fails `make docs-m1-check` with a non-zero exit.
- Network unavailability cannot fail the offline documentation check.
- Production health failure does not make the manual check fail; the manual
  directs the operator to the current runtime truth.
- When a capability loses its prerequisite or verification, its readiness label
  is downgraded and the limitation is recorded instead of preserving an
  optimistic claim.
- When implementation and manual disagree on a stable contract, code is treated
  as observed behavior and the discrepancy blocks documentation acceptance.
- Historical evidence remains linked and dated; it is not rewritten as a live
  status claim.

## 9. Verification and acceptance

The implementation is accepted when:

1. The manual contains all ten sections and the capability matrix contract.
2. A new operator can follow the five-minute check and distinguish local,
   candidate, and production observations.
3. All documented Make targets, local links, routes, and health-check names pass
   the offline checker.
4. Read-only, mutating production, and chaos commands are visibly separated.
5. Every readiness claim contains evidence, limitations, and prohibited use.
6. The real-money and strategy-profitability boundary is explicit.
7. `make docs-m1-check` has focused automated tests and passes from a clean
   checkout without network credentials.
8. The scoped pre-commit behavior is covered by tests for both triggering and
   non-triggering changes.
9. README, learning index, and CURRENT link to the manual with their authority
   boundaries intact.
10. Existing repository documentation and planning guards continue to pass.

## 10. Planned implementation scope

After written-spec approval, the implementation plan may create or update:

- `docs/M1-市场感知平台使用手册.md`;
- an offline manual-contract checker and its focused tests;
- the `docs-m1-check` Makefile target and `make help` entry;
- the scoped `.githooks/pre-commit` integration;
- `README.md`, `docs/learning/00-INDEX.md`, and `.planning/CURRENT.md` links;
- any plan SUMMARY, learning, JOURNAL, and workstream state artifacts required
  by the project's GSD discipline.

This design does not authorize unrelated M1 implementation changes. Discovery
of a real code or production defect while writing the manual must be recorded
and routed separately rather than silently fixed as documentation work.
