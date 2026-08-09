# M1 Operator Surface Alert Routing — Summary

Date: 2026-08-10

## Outcome

Polywatch now treats the Vercel `/perception` page and the Fly
`/perception/console` page as distinct operational surfaces.

- A Vercel HTTP 200 or 302/307 SSO redirect means the protected product route
  is reachable.
- The Fly incident console is the direct on-call gate and must return HTTP 200.
  Its transport or HTTP failure remains an immediate operator-visibility alert.
- Vercel missing-deployment, server, and transport failures remain alerts.

## Operator commands

- `make smoke-perception-dashboard` checks the protected Vercel product route.
- `make smoke-operator-console` checks the direct Fly incident console.

## Verification

- Polywatch and Dashboard contract suites.
- Ruff and M1 manual contract validation.
- Production read-only smoke: Vercel `/perception` returned 302 as expected;
  Fly `/perception/console` returned 200.
