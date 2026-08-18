# Runtime Dashboard Incident Context Design

## Goal

Make a runtime exception immediately understandable from the cloud dashboard as
well as Telegram, without granting the dashboard any control-plane write or
runtime authority.

## Chosen approach

The existing incident ledger remains the sole durable source of truth.  The
control-plane API will project an active runtime incident with its identity,
severity, source, detection time and bounded failure codes.  Historical
runtime events will carry the same operational identity and summary, so a
detected event and its recovery can be read as one incident lifecycle.

The dashboard will render a high-contrast active-incident panel, evidence
freshness with an explicit age, and a chronological incident/recovery ledger.
It will show: incident key, source, severity, first detection time, event
time, affected failure codes, and whether the incident is open or recovered.
The existing unavailable state remains fail-closed.

## Alternatives considered

1. Render only the existing raw failure strings.  This is insufficient: it
   does not identify an incident lifecycle or distinguish observer/source.
2. Add a separate dashboard-only event store.  Rejected: it duplicates durable
   facts and can disagree with Telegram.
3. Project structured fields from the existing ledger (chosen).  It preserves
   one authority and lets dashboard and Telegram describe the same event.

## Boundaries and safety

- No credentials are exposed to the browser.
- Failure detail remains bounded to the writer's existing validated codes.
- The dashboard performs no remediation and writes no incident facts.
- An unavailable API is visibly unavailable, never rendered as an empty
  healthy dashboard.

## Acceptance evidence

1. Postgres projection tests prove active and historical runtime incidents
   include diagnostic fields.
2. Dashboard typecheck proves the rendered contract matches the API contract.
3. A controlled production incident and recovery show the same source,
   affected target and timestamps in Telegram and the dashboard ledger.
