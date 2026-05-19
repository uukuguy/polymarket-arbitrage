---
phase: 02-l1-production-grade
plan: 06
workstream: m1-perception
subsystem: dashboard
tags: [nextjs, vercel, supabase, magic-link, hmac, edge-runtime, rls, monorepo]

requires:
  - phase: 02-03
    provides: Supabase Postgres mirror (snapshots + markets_latest tables, anon-SELECT RLS) — dashboard reads from these
  - phase: 02-04
    provides: Fly daemon /scan endpoint live at polyarb-l1.fly.dev — Vercel Edge Function HMAC-forwards here
  - phase: 02-05
    provides: SCAN_SHARED_SECRET pattern in Fly secrets — same value mirrored to Vercel (different env-var name per BLOCKER-3)
  - phase: 02-08
    provides: Alembic 002_add_top_movers_view.py — /movers page consumes this

provides:
  - "Vercel-hosted Next.js 15 App Router dashboard at https://polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app"
  - "3 read-only pages (/status timeline, /movers uncertainty proxy, /scan recipe trigger) + magic-link auth + email whitelist gate"
  - "Vercel Edge Function /api/scan that HMAC-signs request body with SCAN_SHARED_SECRET and forwards to Fly /scan"
  - "Browser/server Supabase client split (lib/supabase-browser.ts vs lib/supabase-server.ts) to satisfy Next.js 15 'next/headers' boundary"
  - "5 Vercel env vars + 2 Supabase Auth URL config (Site URL + Redirect URLs) — production-ready dashboard"
  - "Daemon-to-dashboard end-to-end flow verified live: anon /status reads real Supabase snapshots; logged-in /scan + magic link triggers Fly recipe and returns JSON"

affects:
  - phase: 02-07 (Wave 5 soak) — dashboard observability surface during 7-day soak, recipe-trigger latency baseline
  - all future M1 phases that ship UI — Next.js scaffold + Supabase auth + Edge-Function HMAC pattern is reusable

tech-stack:
  added:
    - "next@15.5.18 (App Router + Edge Runtime)"
    - "react@19, react-dom@19"
    - "@supabase/supabase-js@2.106 + @supabase/ssr@0.6.1 (server-side cookie auth)"
    - "@types/node@22, @types/react@19, typescript@5.9, eslint@9 (devDeps)"
    - "pnpm@9 (lockfile committed, packageManager field set)"
  patterns:
    - "Server/Client Supabase client split — separate -browser.ts (createBrowserClient) and -server.ts (createServerClient + cookies()) files to avoid Next.js's 'next/headers in Client Component' build error"
    - "Edge-Runtime HMAC signing via Web Crypto (crypto.subtle.importKey + sign) — not Node's 'crypto' module (unavailable in Edge Runtime)"
    - "Middleware whitelist gate — /scan/* and /api/scan/* paths re-validate Supabase session email against EMAIL_WHITELIST env (server-side check, not just client-side)"
    - "Schema fidelity over UI fanciness — when /status executor draft selected phantom columns (parquet_r2_url, supabase_mirror_at_ms, is_valid), we adapted UI to actual Alembic 001 schema instead of adding columns to satisfy executor's draft"

key-files:
  created:
    - "dashboard/package.json + pnpm-lock.yaml — Next.js 15 + Supabase pinned"
    - "dashboard/app/layout.tsx — root layout with nav (/status /movers /scan)"
    - "dashboard/app/page.tsx — index (currently minimal landing)"
    - "dashboard/app/status/page.tsx — L1 timeline view from Supabase snapshots table"
    - "dashboard/app/movers/page.tsx — uncertainty-proxy mover view from top_movers_view"
    - "dashboard/app/scan/page.tsx — recipe trigger UI (client component, fetch /api/scan)"
    - "dashboard/app/login/page.tsx — magic-link form (signInWithOtp)"
    - "dashboard/app/auth/callback/route.ts — Supabase code exchange → set session cookie → redirect to /scan"
    - "dashboard/app/api/scan/route.ts — Vercel Edge Function: Web Crypto HMAC sign + forward to Fly daemon"
    - "dashboard/lib/supabase-browser.ts — createBrowserClient factory (Client Components)"
    - "dashboard/lib/supabase-server.ts — createServerClient + cookies() (Server Components, Route Handlers)"
    - "dashboard/lib/scan-client.ts — typed fetch helper for /api/scan"
    - "dashboard/lib/types.ts — Snapshot / MarketLatest / TopMoverRow TS types mirroring Alembic 001+002 schema"
    - "dashboard/middleware.ts — Next.js middleware: whitelist + protected-path gate"
    - "dashboard/.env.example — 5 env-var docs (incl. BLOCKER-3 SCAN_SHARED_SECRET name asymmetry note)"
    - "dashboard/.vercelignore — W7 fix: in dashboard/ not repo root (Vercel Root Directory = dashboard/)"
    - "dashboard/next.config.mjs, tsconfig.json, next-env.d.ts, .gitignore — standard Next.js scaffolding"
  modified:
    - "Makefile — 4 new targets: dashboard-dev / dashboard-build / dashboard-typecheck / dashboard-deploy"
    - ".gitignore — exclude dashboard build artifacts (.next/, node_modules/, .vercel/)"
    - "tests/m1-perception/test_makefile_contract.py — 5 new Makefile contract tests (dashboard targets exist + invoke correct commands)"
    - ".git/config — REMOVED stale [user] section pointing to fabricated PolyArb Developer <firmwwwee@fastmail.com> identity (unblocks Vercel author verification)"

key-decisions:
  - "Authoritative Supabase schema = Alembic 001 (8 snapshot columns) NOT executor's draft (3 phantom columns). /status UI adapted to existing schema; phantom fields rejected. Reasoning: schema add-only discipline (LEARNINGS P7) — we don't grow the table to satisfy a UI draft; we make the UI match what's already shipped."
  - "/movers spec deviation (vs Plan 02-06 plan): plan called for cross-snapshot mid_price delta requiring markets_history table. Actual schema is markets_latest (full-overwrite, no history). Solution: Plan 02-08 already shipped Alembic 002 top_movers_view as proximity-to-0.5 uncertainty proxy. /movers consumes that. True cross-snapshot delta deferred to Phase 02.1 (needs markets_history sister table)."
  - "Supabase client split into browser-only / server-only modules — forced by Next.js 15 strict 'next/headers in Client Component' compile error. Original single-file mixed factory broke `pnpm build` even though `pnpm typecheck` passed. Lesson: typecheck is not enough for App Router; build is authoritative."
  - "Vercel author verification (Hobby tier strict) blocked deploy of commits authored by firmwwwee@fastmail.com. Mitigation path C chosen: remove project-level .git/config [user] section so new commits inherit global identity (uukuguy@gmail.com). Did NOT rewrite history (would invalidate 173 SUMMARY-referenced SHAs across project)."
  - "Vercel Authentication 'Standard Protection' remains ON (per Wave 4 default). Implication: anon visitors can't see dashboard; magic-link must be clicked in a browser already logged into Vercel. Acceptable for single-user-now phase; revisit when sharing dashboard externally."

patterns-established:
  - "Pattern A — Vercel/Fly env-var name asymmetry: BLOCKER-3 fix. Same value, different name on each side. Vercel: SCAN_SHARED_SECRET (no prefix; dashboard doesn't load pydantic-settings). Fly: POLYARB_SCAN_SHARED_SECRET (POLYARB_ prefix). Documented in dashboard/.env.example + cross-validated via flyctl secrets list digest."
  - "Pattern B — Edge Runtime HMAC: import key via crypto.subtle.importKey('raw', SECRET, {name:'HMAC',hash:'SHA-256'}, false, ['sign']) then crypto.subtle.sign + Buffer.toString('hex') replacement (Edge uses TextEncoder + manual hex conversion). Different from Node 'crypto.createHmac' — required because /api/scan declares `export const runtime = 'edge'` for global low-latency."
  - "Pattern C — Whitelist gate in middleware.ts, not in page.tsx: server-side check before any page renders. Middleware checks Supabase session cookie + reads EMAIL_WHITELIST env + redirects /login?error=not_whitelisted if mismatch. Prevents UI from briefly flashing protected content during client-side hydration."
  - "Pattern D — Schema mismatch debug protocol: when Supabase rejects a SELECT with 'column does not exist', do NOT add the column. First read Alembic migration files (source of truth), then adapt the SELECT to match existing columns. This caught what would have been a phantom Alembic 003 migration adding fields nobody writes."

requirements-completed:
  - "Next.js 15 App Router + Supabase JS SDK + magic-link auth + email whitelist single user (D-19/D-20)"
  - "/status page — L1 timeline read from Supabase snapshots table (D-18)"
  - "/movers page — top markets from Supabase (deviation: uncertainty proxy not cross-snapshot delta; documented in plan key-decisions)"
  - "/scan page — recipe trigger button → Vercel Edge Function → Fly public /scan (D-22 amendment)"
  - "Vercel Edge Function computes HMAC of body + posts to Fly /scan with X-Signature header"
  - "BLOCKER-3 (revision 2026-05-12): Vercel env var SCAN_SHARED_SECRET / Fly env var POLYARB_SCAN_SHARED_SECRET / VALUE byte-identical"

duration: ~3h end-to-end (~50 min executor + ~30 min .git/config troubleshooting + ~45 min Vercel Import + ~30 min Supabase + ~15 min schema/auth verification)
completed: 2026-05-19
---

# Plan 02-06: Vercel Dashboard Live

**A read-only Next.js dashboard reads Polymarket L1 snapshots from Supabase, gates /scan behind email-whitelisted magic-link auth, and HMAC-forwards recipe triggers through a Vercel Edge Function to the Fly daemon — three independent services, signed messages between them, single-user verified end-to-end.**

## Performance

- **Duration:** ~3h across two sessions
- **Started:** 2026-05-18 (executor dispatch)
- **Completed:** 2026-05-19 ~14:30 UTC+8
- **Tasks landed:** 5 (3 autonomous code + 1 human checkpoint + 1 SUMMARY)
- **Files modified:** 24 (+4505 lines; +3442 from pnpm-lock.yaml)
- **Production URL:** `polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app`

## Accomplishments

- **Three-service end-to-end flow verified live**: browser → Vercel dashboard → Vercel Edge `/api/scan` → HMAC sign → Fly `/scan` → daemon recipe → JSON back through the chain. Test executed: pick `thick-but-slippery` recipe → click Run → see JSON result. All four boundary types (browser↔Vercel server / Vercel server↔Vercel Edge / Vercel Edge↔Fly via HMAC / Fly↔Supabase via service_role) green.
- **Magic-link auth working**: signInWithOtp from /login → Supabase emails magic link → click in browser → /auth/callback exchanges code for session cookie → redirects to /scan → whitelist middleware passes (email matches EMAIL_WHITELIST env). Tested with user's whitelisted email; unwhitelisted email correctly redirected to /login?error=not_whitelisted.
- **/status reads real Supabase data**: After schema-mismatch fix (74c61e7), the page renders the actual snapshots timeline (8 Alembic 001 columns) including issue_count_by_layer summary. No fail-soft banner.
- **/movers operational** via Plan 02-08's top_movers_view: 20+ uncertainty-proxy markets ranked by proximity to 0.5 mid_price, including question_zh translations.
- **Deploy author block resolved** without rewriting 173 commits of git history. Removed project-level `.git/config [user]` section that pointed to fabricated `firmwwwee@fastmail.com` identity Claude had set on 2026-04-29; new commits auto-inherit global `uukuguy@gmail.com` which is bound to the GitHub account — Vercel author verification passes for new deploys.

## Task Commits

Each task committed atomically:

1. **Task 1: Next.js scaffold + Supabase magic-link auth + whitelist middleware** — `a26ae74 feat(02-06)`
2. **Task 2: /status + /movers pages + Vercel Edge /api/scan HMAC forwarder** — `7ca96e6 feat(02-06)`
3. **Task 3: Makefile dashboard targets + dashboard/.vercelignore (W7)** — `7f764e6 feat(02-06)`
4. **Task 4 (human checkpoint, dashboard config):**
   - 4.1 fix: `04cfe3b fix(02-06): split lib/supabase.ts into browser + server modules` — Next.js 15 next/headers boundary violation caught at build time
   - 4.2 fix: `74c61e7 fix(02-06): /status page selects only columns that exist in Alembic 001 schema` — phantom-column SELECT removed
   - 4.3 deploy unblock: `8d89eb3 docs(02-06): clarify SCAN_SHARED_SECRET name asymmetry` — small follow-up commit authored by uukuguy@gmail.com (Vercel-passed identity) after stale .git/config [user] cleanup, triggered the auto-redeploy that finally succeeded
   - Vercel project created at polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app with Root Directory = dashboard/, 5 env vars (NEXT_PUBLIC_SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY, EMAIL_WHITELIST, SCAN_SHARED_SECRET, SCAN_ENDPOINT_URL)
   - Supabase URL Configuration: Site URL + /auth/callback added to Redirect URLs allowlist
   - End-to-end verified: /status real data / /movers uncertainty ranking / /scan magic-link → recipe → JSON
5. **Task 5 (this SUMMARY)** — atomic with metadata.

## Files Created / Modified

### Created

- **`dashboard/app/`** (8 routes / route handlers):
  - `layout.tsx` (47 lines) — nav header, dark theme baseline
  - `page.tsx` (6 lines) — index placeholder
  - `status/page.tsx` (127 lines) — Server Component reading Supabase snapshots; renders timeline table or fail-soft banner
  - `movers/page.tsx` (113 lines) — Server Component reading top_movers_view; documented deviation from cross-snapshot delta spec inline
  - `scan/page.tsx` (146 lines) — Client Component; recipe picker + fetch /api/scan + JSON result rendering
  - `login/page.tsx` (99 lines) — Client Component; signInWithOtp form
  - `auth/callback/route.ts` (42 lines) — Route Handler; exchanges code for session, redirects to /status (default) or `next` param
  - `api/scan/route.ts` (74 lines) — Edge Function (`runtime = 'edge'`); HMAC-SHA-256 sign body via Web Crypto, POST to SCAN_ENDPOINT_URL with X-Signature header, return JSON
- **`dashboard/lib/`** (4 helpers):
  - `supabase-browser.ts` (14 lines) — createBrowserClient (Client Components only)
  - `supabase-server.ts` (34 lines) — createServerClient + cookies() (Server Components + Route Handlers only; never imported by Client Components — would break build)
  - `scan-client.ts` (20 lines) — typed fetch wrapper for /api/scan
  - `types.ts` (62 lines) — Snapshot / MarketLatest / TopMoverRow TS types reflecting Alembic 001+002 schema
- **`dashboard/middleware.ts`** (64 lines) — Next.js middleware running on /scan/:path* and /api/scan/:path*: pulls Supabase session, checks email against EMAIL_WHITELIST, redirects to /login or /login?error=not_whitelisted; matcher excludes /auth/callback, /login, /, /status, /movers (public routes)
- **`dashboard/.env.example`** (18 lines) — 5 env vars + BLOCKER-3 note about Vercel/Fly env-var-name asymmetry
- **`dashboard/.vercelignore`** (10 lines) — W7 fix: lives in dashboard/ not repo root because Vercel Root Directory is dashboard/
- **`dashboard/{next.config.mjs, tsconfig.json, next-env.d.ts, package.json, pnpm-lock.yaml, .gitignore}`** — standard Next.js 15 scaffolding

### Modified

- **`Makefile`** (+26 lines) — 4 new targets: `dashboard-dev` (next dev), `dashboard-build` (next build), `dashboard-typecheck` (tsc --noEmit), `dashboard-deploy` (vercel deploy --prod). All take optional ARGS env var for forwarding flags.
- **`.gitignore`** (+7 lines) — exclude `dashboard/.next/`, `dashboard/node_modules/`, `dashboard/.vercel/` so build artifacts don't bloat commits
- **`tests/m1-perception/test_makefile_contract.py`** (+72 lines, 5 new tests) — assert make -n dashboard-{dev,build,typecheck,deploy} exit 0 and contain correct subcommands
- **`.git/config`** (REMOVED `[user]` section) — was setting commit author to fabricated `PolyArb Developer <firmwwwee@fastmail.com>` identity Claude planted on 2026-04-29. New commits now use global identity `Jiangwen Su <uukuguy@gmail.com>`. Did not rewrite the 173 historical commits authored by the fabricated identity — see deferred-items.

## /status Schema Adaptation (74c61e7 fix)

The executor's initial draft of `dashboard/app/status/page.tsx` selected 3 Supabase columns that don't exist in the shipped schema:

| Column the draft expected | Reality (Alembic 001) | What we did |
|---|---|---|
| `parquet_r2_url` | only `parquet_url` exists | Replaced with `parquet_url` presence as "r2?" indicator |
| `supabase_mirror_at_ms` | not in snapshots table | Dropped the column |
| `is_valid` | not in snapshots table | Dropped the column |

The new UI shows: `taken_at` / `mode` / `status` / `markets` / `r2?` / `issues`. The `issues` cell shows a compact `L1:0 L2:3 L4:12` summary from `issue_count_by_layer` (Alembic 001 JSON column).

## Lib Split (04cfe3b fix)

`pnpm typecheck` was clean but `pnpm build` failed with:

```
Error: You're importing a component that needs "next/headers". That only works
in a Server Component which is not supported in the pages/ directory.
```

Root cause: `dashboard/lib/supabase.ts` was a single file exporting BOTH `getBrowserSupabase()` and `getServerSupabase()`. `getServerSupabase()` transitively imported `next/headers` (Server-only). When `app/login/page.tsx` (Client Component) imported `getBrowserSupabase`, Next.js refused because the same module imports server-only API.

Fix: split into two files. Importers updated:
- `app/login/page.tsx` → `@/lib/supabase-browser`
- `app/status/page.tsx`, `app/movers/page.tsx` → `@/lib/supabase-server`
- `middleware.ts` already inlined `createServerClient` (no change needed)

## Vercel Deploy Author Verification Block

Initial commits authored by `PolyArb Developer <firmwwwee@fastmail.com>` were rejected by Vercel's commit-author-to-GitHub-account match check. Hobby tier doesn't expose a UI toggle to disable this.

Mitigation chosen (path C from session discussion): **remove the stale project-level `.git/config [user]` section**. New commits then inherit global identity `Jiangwen Su <uukuguy@gmail.com>` (bound to `uukuguy` GitHub account). One follow-up commit (`8d89eb3 docs(02-06)`) was authored under the correct identity, pushed, and Vercel auto-deployed it successfully.

The 173 historical commits authored by the fabricated identity remain in `git log` (deferred — rewriting them would invalidate every SHA referenced in SUMMARY documents across phase 01.1 / 02).

## Known Limitations + Deferred Items

1. **`/movers` is NOT true cross-snapshot delta** (deferred to Phase 02.1):
   - Plan called for `m1.mid_price - m0.mid_price` between latest 2 OK snapshots
   - Shipped schema (markets_latest, full-overwrite) has no markets_history
   - Plan 02-08 Alembic 002 built `top_movers_view` as proximity-to-0.5 uncertainty proxy — useful but different signal
   - When markets_history is built (Phase 02.1), `/movers` query swaps to JOIN-on-snapshot_id

2. **Vercel Authentication 'Standard Protection' is ON** (Wave 4 prep default):
   - Anon visitors get 401 on every path
   - Magic-link only works in a browser already logged into Vercel (otherwise user can't even reach /login)
   - When dashboard needs to be shared externally (e.g., partner review): toggle to "Only Preview Deployments" or "Disabled" via Settings → Deployment Protection → Vercel Authentication

3. **173 historical commits still authored by `firmwwwee@fastmail.com`** (deferred indefinitely):
   - Rewriting (rebase --root --exec amend) would invalidate every commit SHA referenced in SUMMARY documents across Phase 01.1 / 02 / 02.x
   - Not security-critical: repo is private, no secrets in those commits, Vercel author verification only checks the deployed commit not history
   - Future Vercel deploys are unaffected because new commits use the correct identity

4. **3 pre-existing test failures unrelated to Plan 02-06** continue to be deferred (test_makefile_contract `make_smoke_health_local`, test_pass_when_fresh, test_r2_retry) — see `.planning/.../02-l1-production-grade/deferred-items.md`

5. **Sentry/dashboard error tracking NOT wired**:
   - Plan 02-05 added Sentry on the daemon side
   - Dashboard side could reuse same Sentry project via `NEXT_PUBLIC_SENTRY_DSN` (Vercel env var) + sentry/nextjs SDK — deferred. Right now dashboard errors only surface in Vercel runtime logs.

## Production Configuration Snapshot

| Component | Value (Production) |
|---|---|
| Vercel project | `polymarket-arbitrage` (Hobby tier) |
| Deployment URL | `polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app` |
| Root Directory | `dashboard/` |
| Framework Preset | Next.js |
| Node Version | (Vercel default = 20.x) |
| Vercel env vars | NEXT_PUBLIC_SUPABASE_URL / NEXT_PUBLIC_SUPABASE_ANON_KEY / EMAIL_WHITELIST / SCAN_SHARED_SECRET / SCAN_ENDPOINT_URL |
| Vercel Authentication | Standard Protection (Require Log In: ON) |
| Supabase Auth Site URL | `https://polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app` |
| Supabase Auth Redirect URLs | `https://...vercel.app/auth/callback` |
| Routes | `/` (○ static) `/login` (○ static) `/scan` (○ static) `/_not-found` (○ static) `/status` (ƒ dynamic) `/movers` (ƒ dynamic) `/api/scan` (ƒ Edge) `/auth/callback` (ƒ dynamic) |

## Plan 02-07 Handoff

Plan 02-07 (Wave 5 — chaos + 7-day soak) starts from this state:
- Three live services with verified inter-service auth: Fly daemon (HMAC) / Supabase (RLS anon-SELECT + service_role write) / Vercel dashboard (magic-link + EMAIL_WHITELIST middleware)
- Three alert paths E2E verified (per Plan 02-05): Sentry email + Better Stack email + Telegram direct
- `make sentry-test` / `make alerts-test` / `make dashboard-build` all functional for smoke tests
- Soak chaos targets to add: Vercel Edge timeout > Fly idle / Fly daemon 429 from Polymarket Gamma / Supabase RLS auth_key rotation / Telegram bot rate limit
