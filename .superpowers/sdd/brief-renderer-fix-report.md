# Business brief renderer fix report

## Scope

Changed only the human-readable `render_business_brief` projection.  The
canonical `build_business_brief` JSON object remains unchanged.

## Result

- Replaced raw mapping/list rendering with labelled scalar lines for Structure,
  Quote, qualification, opportunities, incidents, and watchdog state.
- Missing, `None`, and composite selected values render as `未提供`.
- Renders at most five opportunity lines and at most three runtime-incident
  summaries; an authoritative empty opportunity list displays `暂无认证机会`.
- The renderer never emits a Python dictionary representation.

## Verification

- `uv run pytest tests/m1-perception/test_business_brief.py -q` — 13 passed
- `uv run ruff check src/polyarb/control_plane/business_brief.py tests/m1-perception/test_business_brief.py` — passed
- `uv run pyright src/polyarb/control_plane/business_brief.py` — 0 errors
- `make control-plane-business-brief` — passed against the live authorities;
  output used labelled scalar lines and contained no dictionary representation.
