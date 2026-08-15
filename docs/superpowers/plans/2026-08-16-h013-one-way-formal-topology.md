# H-013 — one-way transactional formal topology

## Goal

Make the transactional control-plane deployment artifact represent the proven
five-role topology that will become the sole formal M1 runtime: one coordinator,
two independently identified structure-range workers, and two independently
identified quote-batch workers.

## Constraints

- Reuse the existing transactional Supabase/Postgres and R2 authority in place.
- Do not create a third Supabase project or change the subscription.
- Do not use legacy L1 as a fallback, rollback, or runtime path.
- No cloud action is permitted in the renderer; deployment remains an explicit
  subsequent operational step.

## Steps

1. Add red tests asserting the five named process identities, production-sized
   VM, and the one-way formal-promotion checklist step.
2. Update the Fly Worker template and local rollout artifact accordingly.
3. Run focused deployment/rollout tests, full Climb checks, and record the
   plan Summary before beginning the formal cloud deployment.
