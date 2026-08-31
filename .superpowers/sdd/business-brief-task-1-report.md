# Business brief Task 1 report

## Commit

`dffaa3df feat(m1): add canonical business brief`

## Files

- `src/polyarb/control_plane/business_brief.py`
- `tests/m1-perception/test_business_brief.py`

## TDD evidence

### RED

Command:

```bash
uv run pytest tests/m1-perception/test_business_brief.py -q
```

Result before implementation: collection failed with
`ModuleNotFoundError: No module named 'polyarb.control_plane.business_brief'`.

### GREEN

Command:

```bash
uv run pytest tests/m1-perception/test_business_brief.py -q
```

Result after implementation: `4 passed`.

## Delivered behavior

- Strictly validates the available status and opportunities authorities.
- Produces the specified fixed brief mapping, including a five-item opportunity cap.
- Raises `BusinessBriefUnavailable` for unavailable or malformed authority input.
- Renders the five required labelled business sections without deriving edge totals or P&L.

## Concerns

- The renderer intentionally displays the supplied canonical facts directly; presentation details beyond the five labels are deferred to the CLI/rendering task.

## Review-fix follow-up

### Root cause

The control-plane status authority exposes `runtime_incidents` and
`recovery_actions` as bounded collection mappings (`items` and `total`), but
the first implementation incorrectly required each to be a sequence. In
addition to rejecting real status responses, using the truthiness of a mapping
would have escalated even an empty `{\"items\": [], \"total\": 0}` response.

### Delivered fix

- Validate and preserve both control-plane collection mappings unchanged.
- Validate non-negative totals and reject totals smaller than their returned
  item count.
- Base runtime escalation on `runtime_incidents.total`, so a bounded empty
  page with a positive total still escalates and an empty collection does not.
- Update the fixture to the real control-plane shape and narrow nested-object
  assertions for Pyright.

### Verification

```bash
uv run pytest tests/m1-perception/test_business_brief.py -q
# 5 passed
uv run ruff check src/polyarb/control_plane/business_brief.py tests/m1-perception/test_business_brief.py
# All checks passed!
uv run pyright src/polyarb/control_plane/business_brief.py tests/m1-perception/test_business_brief.py
# 0 errors, 0 warnings, 0 informations
git diff --check
```
