# L3 Soak Auto-checkpoint Operator Plan

> **For agentic workers:** Execute inline in the existing
> `fix/l3-continuity-repair` worktree. Do not dispatch subagents.

**Goal:** Generate, validate, commit, and push the selected release-75
T0/T6/T12/T18/T24 continuity reports without a foreground terminal wait.

**Architecture:** A user-domain macOS `launchd` agent runs every five minutes.
An external operator script reads the immutable repository manifest, checks the
exact Fly identity, obtains the least-privileged runtime password from
Keychain, and invokes the existing Makefile verifier. Production monitoring
remains on Fly and is not modified.

**Tech Stack:** macOS `launchd`, zsh, `security`, `flyctl`, `jq`, `uv`, Git.

## Global Constraints

- Do not restart or redeploy `polyarb-l2`.
- Do not put a database password or DSN in a file, plist, log, or Git.
- Require machine `85e647c4eed598`, instance
  `01KYES89KD9WA8VV9V2B3PJV7R`, digest
  `sha256:f0d39892207577bb024995d76e91f5c0b8c0a88fd8e2839e182d25125da16ad5`,
  and release `9f385cacc104fa54dd444151a8c4ecb423e94dde`.
- Derive all checkpoint bounds and output paths from
  `05.4-SOAK-MANIFEST-20260726T085113Z.json`.
- Never overwrite a canonical report. Stop after the first non-PASS report.
- Keep Fly Polywatch as the live detector and Telegram alert path.

---

### Task 1: Anchor the selected attempt

**Files:**
- Commit:
  `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-MANIFEST-20260726T085113Z-T0.json`
- Modify:
  `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`

- [ ] Verify the T0 report is `PASS`, binds the corrected manifest hash
  `3ad69a90…`, and pins release 75 identity.
- [ ] Record T0/T6/T12/T18/T24 UTC boundaries and the independent resident
  monitor state.
- [ ] Commit and push the T0 evidence before installing the scheduler.

### Task 2: Install the fail-closed user scheduler

**Files:**
- Install:
  `~/Library/Application Support/PolyArb/l3-soak-autocheckpoint.zsh`
- Install:
  `~/Library/LaunchAgents/com.polyarb.l3-soak-autocheckpoint.plist`
- Runtime state:
  `~/Library/Application Support/PolyArb/l3-soak-autocheckpoint.state/`

- [ ] The script must use a non-blocking directory lock so ticks cannot
  overlap.
- [ ] Before a due checkpoint, require a clean worktree and unchanged
  verifier/executable surface relative to release `9f385ca`.
- [ ] Query Fly and `/health`; require exact machine, instance, digest,
  release, and passing L3 sample/membership/freshness checks.
- [ ] Read Keychain service `polyarb-l2-runtime-054`, account
  `zoqsmjeejfkrokwttjbx`; URL-encode the password in memory and unset it after
  each verifier invocation.
- [ ] For each due missing report, invoke `make l3-soak-checkpoint` with the
  manifest-declared bounds/path, validate `status=PASS` and both immutable
  hashes, then commit and push only that report.
- [ ] At T+24, invoke `make l3-soak-verify`, record a local completion marker,
  and send a macOS completion notification.
- [ ] On any failure, write only redacted diagnostics and send a macOS failure
  notification. Do not create a replacement manifest.

### Task 3: Preflight and handoff

**Files:**
- Inspect:
  `~/Library/Logs/polyarb-l3-soak-autocheckpoint.log`

- [ ] Validate the plist with `plutil -lint`.
- [ ] Bootstrap the LaunchAgent and kick one immediate tick.
- [ ] Require the pre-boundary tick to exit successfully with `next=T+6`,
  create no T6 file, and leave the Git worktree clean.
- [ ] Re-run the Fly resident watcher and require all four production surfaces
  green with no Telegram alert.
- [ ] Run `make planning-status`, documentation checks, and Git status.

### Task 4: Boundary completion

- [ ] T+6 not-before: `2026-07-26T14:51:13.206077Z`.
- [ ] T+12 not-before: `2026-07-26T20:51:13.206077Z`.
- [ ] T+18 not-before: `2026-07-27T02:51:13.206077Z`.
- [ ] T+24 not-before: `2026-07-27T08:51:13.206077Z`.
- [ ] After the final verifier passes, reconcile the five report hashes into
  `05.4-SOAK-LOG.md`, close `05-07-SUMMARY.md`, update ROADMAP/STATE/JOURNAL,
  and run the Phase 05 final verification gates.
