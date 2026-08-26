# Task 3 Report: Exact Qualification Release and Configuration Identity

## RED

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py -q
```

Observed result: exit 1.

Key output:

```text
FFFFFFFFFFFFFFFFFF....FF............................................FF.. [ 86%]
........F..                                                              [100%]
ModuleNotFoundError: No module named 'polyarb.control_plane.qualification_identity'
TypeError: render_rollout_artifacts() got an unexpected keyword argument 'release_id'
AssertionError: assert {'POLYARB_RUN...ontrol-plane'} == {'POLYARB_DB_...ontrol-plane'}
SystemExit: 2
control-plane: error: unrecognized arguments: --release-id 0123456789abcdef0123456789abcdef01234567
AssertionError: must not construct fact source
```

Failure was expected: qualification identity did not exist, renderer/CLI did not accept or render release/config identity, templates lacked fixed identity env, and qualification service construction was not gated by release/config identity.

## GREEN

Command:

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py -q
```

Output:

```text
........................................................................ [ 84%]
.............                                                            [100%]
```

Command:

```bash
uv run ruff check src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py
```

Output:

```text
All checks passed!
```

Command:

```bash
uv run python -m py_compile src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py
```

Output: no stdout/stderr, exit 0.

Command:

```bash
uv run pyright src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py
```

Output:

```text
0 errors, 0 warnings, 0 informations
```

Command:

```bash
git diff --check -- src/polyarb/control_plane/qualification_identity.py src/polyarb/control_plane/rollout.py src/polyarb/cli_control_plane.py deploy/control-plane/fly-runtime-controller.toml.template deploy/control-plane/fly-qualification-worker.toml.template tests/m1-perception/test_control_plane_qualification_identity.py tests/m1-perception/test_control_plane_rollout.py tests/m1-perception/test_control_plane_deployment_templates.py tests/m1-perception/test_control_plane_cli.py
```

Output: no stdout/stderr, exit 0.

## Changes

- Added `src/polyarb/control_plane/qualification_identity.py` with canonical payload generation, `sha256:` config digesting, exact lowercase 40-character release validation, ordered `opportunity,quote,structure` roles, observe-only recovery mode validation, integral cadence normalization, recovery target validation/dedup/sort, and `hmac.compare_digest`.
- Updated rollout rendering to require `release_id`, render artifact version 10 for runtime/qualification topology, record `revisions-022-through-026-migration`, fixed DB roles, revision `026`, canonical qualification payload/digest, exact release ID, observe-only mode, and `cloud_actions_performed=false`.
- Updated runtime controller and qualification worker templates with scoped expected database identity. Qualification worker now carries release/config/role/recovery identity and `--batch-size 100`; rendered allowlist remains empty unless exact targets are supplied.
- Updated CLI `render-rollout` with `--release-id` and changed qualification service construction to validate identity using actual `--interval-seconds` and `--batch-size` before constructing policy, fact source, store, or service. Task 2 daemon DB role gate remains before service construction.
- Added tests for canonical identity, env rejection, digest sensitivity, empty allowlist rendering, template fixed identity, sanitized CLI errors, and identity gate ordering.

## Self Review

- No production, Fly, deployment, secret installation, login role provisioning, restart, downgrade, fault injection, or external cloud action was performed.
- Error paths return stable error classes/reasons and do not echo supplied release values, DSNs, credentials, auth headers, or provider bodies.
- Existing Task 2 DB role gate is preserved for qualification serve and still runs before service construction.
- Renderer checks unresolved `__[A-Z0-9_]+__` placeholders after replacements.
- `.superpowers/sdd/progress.md` was pre-existing dirty state and was not staged.

## Commit

Subject:

```text
feat(05.6-207): bind qualification to release identity
```

## Concerns

- None.
