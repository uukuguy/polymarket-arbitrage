# M1 Structure P1 Dashboard and Slice Recovery Design

## Problem

The Fly-native incident console exposes the current Structure producer failure,
but the Vercel Perception dashboard rejects `market-map-stale` diagnoses in its
strict reader contract. It consequently renders the overview unavailable when
the operator needs the Structure P1 details. The failure is real: a Structure
publication child targets a 45-second cooperative slice and has a 75-second
hard limit, but a normalization write can retain SQLite's 120-second default
writer wait. The parent then has to kill it at 75 seconds.

## Decision

The public incident contract accepts a typed Structure diagnosis and the Vercel
overview promotes a Structure P1. The panel shows impact, failure reason,
failed stage, elapsed time, 45-second checkpoint target, 75-second hard limit,
automatic action, and next operator action.

Publication commits receive an explicit writer-lock timeout smaller than the
remaining cooperative slice margin. Contention returns a classified failure
before the child boundary. Quote priority and the 75-second ceiling remain
unchanged.

## Verification

Use red/green tests for Dashboard contract/rendering and a locked publication
writer. Production acceptance requires a fresh Structure pointer, a later
certified Quote run, no open P1/P2 incident, reachable Dashboard, and
Polywatch evidence for fault and recovery.
