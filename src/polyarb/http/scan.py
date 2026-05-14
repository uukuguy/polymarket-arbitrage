"""POST /scan endpoint + HMAC X-Signature middleware.

Phase 02 Plan 02 — D-21 / D-22 / T-02-01 / T-02-02.

D-22 amendment: /scan is PUBLIC but HMAC-protected (not Flycast-internal).
Vercel Edge Functions are cross-org → cannot reach Flycast. Auth gate is
X-Signature HMAC-SHA256 middleware (Stripe/GitHub webhook pattern).

Security:
- hmac.compare_digest (constant-time) prevents timing oracle attacks
- recipe_name validated: isinstance(str) + len ≤ 64 → KeyError/dict lookup only
- ALL SQL goes through run_recipe / run_recipe_grouped (Phase 01.1 P1 trust-split)
  NO parallel SQL path exists. This is the hard D-21 + P1 requirement.
- Layer 2/3 validators are re-run for yaml recipes via list_all_recipes
  (scan_auth_middleware does body auth; scanner.py does SQL auth)

Source: RESEARCH.md §9 lines 1402-1465 (adapted)
"""
from __future__ import annotations

import hashlib
import hmac
import sqlite3
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.observation.scanner import list_all_recipes, run_recipe, run_recipe_grouped


async def scan(request: Request) -> JSONResponse:
    """POST /scan — invoke a Phase 01.1 builtin or yaml recipe.

    Body: {"recipe_name": "thick-but-slippery", "params": {}}

    Returns: {"recipe": str, "row_count": int, "rows": list[dict]}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    recipe_name = body.get("recipe_name")
    body.get("params", {})  # params accepted but not yet used (reserved for future)

    # Input validation: recipe_name must be str ≤ 64 chars
    if not isinstance(recipe_name, str) or len(recipe_name) > 64:
        return JSONResponse({"error": "invalid recipe_name: must be str ≤ 64 chars"}, status_code=400)

    settings = request.app.state.settings

    # Resolve yaml path (use None if file doesn't exist — builtins only)
    yaml_path: Path | None = None
    if settings.recipes_yaml_path.exists():
        yaml_path = settings.recipes_yaml_path

    # list_all_recipes merges BUILTIN_RECIPES + yaml (with Layer 2/3 validation at load)
    recipes = list_all_recipes(yaml_path)

    if recipe_name not in recipes:
        return JSONResponse({"error": f"unknown recipe: {recipe_name!r}"}, status_code=404)

    recipe = recipes[recipe_name]
    db_path = settings.db_path

    try:
        # P1 trust-split: run_recipe / run_recipe_grouped is the ONLY SQL path
        # (Layer 1 read-only URI + Layer 2/3/4 validators all enforce here)
        if recipe.group_by:
            df = run_recipe_grouped(db_path, recipe)
        else:
            df = run_recipe(db_path, recipe)
    except KeyError as e:
        return JSONResponse({"error": f"unknown recipe: {e}"}, status_code=404)
    except ValueError as e:
        # Layer 2/3/4 validation failure (forbidden token / order_by whitelist / limit range)
        return JSONResponse({"error": f"Layer 2/3/4 validation failed: {str(e)[:200]}"}, status_code=400)
    except sqlite3.OperationalError as e:
        return JSONResponse({"error": f"database error: {str(e)[:200]}"}, status_code=500)

    return JSONResponse(
        {
            "recipe": recipe_name,
            "row_count": len(df),
            "rows": df.head(100).to_dict(orient="records"),
        }
    )


async def scan_auth_middleware(request: Request, call_next: Any, *, secret: str) -> Any:
    """HMAC-of-body auth for /scan; bypass /health (D-22 amendment: /health is public).

    Pattern: Stripe/GitHub/Shopify webhook HMAC-SHA256 validation.
    - hmac.compare_digest: constant-time comparison (prevents timing oracle, T-02-01)
    - Reject with 401 on missing or invalid X-Signature header

    Secret handling:
    - Fly daemon: POLYARB_SCAN_SHARED_SECRET env var (pydantic Settings.scan_shared_secret)
    - Vercel Edge: SCAN_SHARED_SECRET env var (no POLYARB_ prefix — not pydantic Settings)
    - Both sides: hmac-sha256(body_bytes, secret.encode('utf-8')) → hex

    Note: if secret is empty (test mode with POLYARB_ALLOW_EMPTY_SECRET=1),
    HMAC validation still runs but any non-empty signature will fail — tests
    must use make_signed_request helper to compute the correct signature.
    """
    if request.url.path != "/scan":
        # /health and any other path bypass auth
        return await call_next(request)

    received_sig = request.headers.get("X-Signature")
    if not received_sig:
        return JSONResponse({"error": "missing X-Signature header"}, status_code=401)

    # Accept both Stripe/GitHub webhook format `sha256=<hex>` AND bare `<hex>`.
    # The docstring above promises Stripe/GitHub pattern; without this strip,
    # any client following the documented pattern would 401. Bare-hex form
    # preserved for backward compat with existing tests + ad-hoc curl clients.
    if received_sig.startswith("sha256="):
        received_sig = received_sig[len("sha256=") :]

    # Read body for HMAC computation
    body = await request.body()

    # Compute expected HMAC-SHA256 of body bytes with shared secret
    expected_sig = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    # Constant-time compare (T-02-01: prevents timing oracle)
    if not hmac.compare_digest(received_sig, expected_sig):
        return JSONResponse({"error": "invalid X-Signature"}, status_code=401)

    # Re-inject body for downstream scan() handler
    async def _receive() -> dict:
        return {"type": "http.request", "body": body, "more_body": False}

    request._receive = _receive  # type: ignore[assignment]

    return await call_next(request)
