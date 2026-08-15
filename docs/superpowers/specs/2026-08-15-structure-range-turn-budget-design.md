# Structure Range Turn Budget Design

## Problem

The v3 Structure manifest creates one bounded range per component shard. The
current scheduler treats `max_turns` as a cap over its eight distinct worker
kinds, so a service configured with eight turns gives a large range backlog
exactly one Structure range turn every tick. With a two-second cadence, the
observed 1,016-range generation takes roughly 34 minutes to drain and cannot
keep pace with continuous source windows.

## Decision

Add `structure_range_turns` to `TransactionalControlPlaneScheduler`. Each tick
keeps one ordered turn for every existing worker kind, then runs up to the
configured number of additional Structure range turns. The default is zero,
preserving all existing deployments and tests. Staging will set the budget to
eight only after the code is deployed.

Every additional range turn remains sequential within a process and calls the
unchanged `TransactionalStructureWorker.run_once()`. Postgres remains the only
cross-process ownership authority: every turn independently claims a lease,
and an empty queue returns `idle` without side effects.

## Alternatives Rejected

1. Raise `max_turns`: currently ineffective because it is capped at eight
   worker kinds; changing it to repeat every worker would multiply Gamma and
   source traffic as well as range work.
2. Add another always-on staging machine: validates multi-worker fencing but
   adds cost and source load without removing the scheduler's range bias.

## Invariants

- Existing workers each get one turn per tick regardless of range backlog.
- Extra turns are only `structure-range`; source, materializer, certifier, and
  Quote call volume does not increase.
- Local turns remain serial and individually bounded by the existing timeout.
- Cross-process safety, idempotency, and retries remain governed exclusively
  by existing Postgres lease fences.
- `structure_range_turns=0` preserves previous turn ordering and output.

## Acceptance Evidence

1. Scheduler contracts prove default compatibility, one base turn per worker,
   and exactly N extra range turns in the same tick.
2. A timeout in one extra range turn does not prevent later turns or cause
   overlap.
3. Staging with budget eight demonstrates materially faster receipt drain,
   no lease conflicts, no new incomplete-generation incidents, bounded RSS,
   and zero publication-pointer mutation before certification.
