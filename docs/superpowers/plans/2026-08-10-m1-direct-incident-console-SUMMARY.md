# M1 Direct Incident Console — Summary

Date: 2026-08-10

## Problem

The Vercel Dashboard `/perception` route is protected by an operator login
redirect. During a production incident, that redirect made the existing rich
Dashboard the only unavailable place to inspect diagnosis and actions.

## Change

`GET /perception/console` is a Fly-native, public, read-only operator console.
It refreshes the already-public bounded incident envelope every 30 seconds and
renders only through DOM `textContent`:

- severity, incident kind/state and scope;
- impact, automatic action and next operator action;
- failure reason, retry evidence and next retry;
- recovery/current evidence as JSON.

If the bounded API returns an error, the console says this is an
operator-visibility failure. It never displays an empty incident list as a
successful result.

## Verification

- RED: `GET /perception/console` returned 404 before the route existed.
- GREEN: `tests/m1-perception/test_perception_http.py::test_perception_console_is_a_direct_operator_view` passes.
- Ruff passes for the changed app, console and contract test.

## Operator URL

`https://polyarb-l1.fly.dev/perception/console`

The Vercel Dashboard remains an optional richer interface. This endpoint is
the direct path when Vercel authentication or deployment is unavailable.
