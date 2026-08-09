# M1 Capacity Recovery Activation — Summary

Date: 2026-08-10

## Problem

The 100GB Fly volume had recovered to more than 50% free space after expansion,
but the durable `capacity-pressure` incident remained in `recovering`. The
capacity controller that records the required receipt-gated normal recovery was
implemented but disabled in L1 production.

## Change

`fly.toml` now sets `POLYARB_CAPACITY_CONTROLLER_ENABLED=true`.

The resident controller measures every 30 seconds, yields to active/due Quote
collection, and runs only the existing bounded snapshot/Quote cleanup under a
pressure state. It does not run VACUUM or unbounded deletion. An open capacity
incident may close only after a positive reclaim receipt and later normal
measurement; volume expansion by itself remains insufficient evidence.

## Verification

- RED: the Fly release contract lacked the enable flag.
- GREEN: the release contract, capacity policy/controller/lifecycle, and daemon
  wiring tests pass (11 tests), and Ruff passes.

## Production gate

After deployment, verify `perception:capacity_controller` reports active
measurements and inspect `/perception/incidents` until the existing incident
has either a verified receipt-backed closure or an explicit continuing fault.
