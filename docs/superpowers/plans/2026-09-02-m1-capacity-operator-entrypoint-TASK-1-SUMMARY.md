# M1 Capacity operator entrypoint — Task 1 Summary

## Outcome

`make control-plane-capacity` is now a first-class daily operator command. It
returns only the current database budget state and largest relations, instead
of requiring an operator to parse the larger job/incident snapshot.

## Implementation

- Added the read-only `capacity` control-plane CLI subcommand.
- Added the Makefile target and `.PHONY` declaration.
- The command deliberately calls only the independent capacity probe, so a slow
  operational snapshot cannot hide storage pressure.

## Verification

- Unit coverage proves the command does not invoke `operational_snapshot`.
- `make control-plane-capacity` returns the production capacity diagnostic.

## Production follow-up

Use this command as the capacity panel's source and daily preflight before
resuming a stopped producer lane.
