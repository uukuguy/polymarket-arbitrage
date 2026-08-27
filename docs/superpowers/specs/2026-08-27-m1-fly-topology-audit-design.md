# M1 Fail-Closed Fly Topology Audit Design

## Goal

Replace raw `flyctl status --json` as an operator dependency with one read-only,
allowlisted audit that cannot print ordinary environment values or provider
response bodies.

## Contract

The operator supplies exact `app/Machine` targets and optional exact
`app/SECRET_NAME` requirements. The audit captures Fly JSON in memory, rejects
unexpected topology, and emits only:

- app name;
- exact Machine ID, state, image identity, and process group;
- sorted ordinary environment **key names**;
- booleans for requested secret-name presence.

It never emits environment values, secret values, unrequested secret names,
provider stderr, provider response bodies, or exception detail.

## Fail-closed rules

- App, Machine, state, image, process-group, env-key, and requested secret names
  must match bounded safe identifier grammars.
- Any ordinary environment key whose name indicates a credential (`DSN`,
  `PASSWORD`, `PASSWD`, `TOKEN`, `SECRET`, `PRIVATE_KEY`, `API_KEY`, or
  `ACCESS_KEY`) fails the whole audit. The failure may name the key but cannot
  expose its value.
- Missing/extra Machines, missing required secrets, malformed JSON, unexpected
  provider shapes, command timeouts, and non-zero provider exits fail with a
  stable reason code and bounded identifiers only.
- `FLY_API_TOKEN` is forced empty for every child command so the verified
  Keychain identity cannot be shadowed by a stale project `.env` token.
- The command is read-only and invokes only `flyctl status ... --json` and, when
  required secrets are declared, `flyctl secrets list ... --json`.

## Boundary

This audit grants no deployment, restart, secret mutation, Machine mutation,
recovery, fault injection, database, or trading authority. It is evidence for
later exact authorization packages, not authorization by itself.
