### Task 7: Dashboard Perception and Incident Views

**Goal**

Expose the Task 6 bounded public read models as an operator-facing Dashboard
without turning transport failures into a false zero-opportunity state.

**Files**

- Create: `dashboard/app/perception/page.tsx`
- Create: `dashboard/app/perception/[group_id]/page.tsx`
- Create: `dashboard/lib/perception.ts`
- Modify: `dashboard/app/layout.tsx`
- Modify: `dashboard/lib/types.ts`
- Modify: `Makefile`
- Modify: `scripts/check_m1_manual.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Test: `tests/m1-perception/test_dashboard_perception_contract.py`

**Required interfaces**

- Consume Task 6 public GET endpoints only.
- Produce `/perception` overview.
- Produce `/perception/[group_id]` history.
- Add `make smoke-perception-dashboard`.

**Non-negotiable behavior**

1. The typed reader uses `cache: "no-store"` and a three-second absolute
   timeout.
2. Transport, HTTP, and JSON failures render a typed unavailable warning.
   They never render as a valid zero-opportunity state.
3. The overview distinguishes watching, stale, unavailable, and invalidated
   groups and shows opportunity edge/capacity, Structure and Quote age,
   15/30/60-minute raw and weighted coverage, Discovery/Reconciliation
   progress, resource mode, and open incidents.
4. The group page presents membership revisions, Quote batches, opportunity
   transitions, and incident events on one timestamped timeline.
5. All new executable entry points are exposed through the Makefile and the
   living M1 manual remains synchronized.
6. No production deployment or cutover occurs in Task 7. Deployment remains
   Task 8 scope.

**Execution**

1. Write and run the Dashboard source-contract RED test.
2. Implement the typed fail-soft reader.
3. Implement overview and group-history pages.
4. Add the smoke target and manual route.
5. Run:

   ```bash
   make dashboard-typecheck
   make dashboard-build
   uv run pytest tests/m1-perception/test_dashboard_perception_contract.py -q
   make docs-m1-check
   make planning-status
   ```

6. Commit atomically, write the Task 7 summary, and run an independent
   six-pillar visual/UI review before Task 8.

**Safety boundary**

- Observer-only.
- No wallet, signing, order placement, balance mutation, or real-money action.
- No production deployment in this task.
