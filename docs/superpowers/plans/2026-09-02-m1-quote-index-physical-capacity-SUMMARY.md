# M1 Quote Index Physical Capacity — Implementation Summary

**Completed:** 2026-09-02

## Outcome

Quote candidate research is now isolated from the published business reader and
each certified replacement transaction physically resets both the former active
index and the candidate staging relation. Dashboard readers continue to read
only `m1_business_quote_rows`.

## Delivered

- Alembic revision `045` creates `m1_business_quote_staging_rows` and grants
  only the control-plane runtime privileges needed to stage, promote and clear
  candidate data.
- Quote batch receipts write research rows to staging. The fenced certifier
  checks the generation, truncates the former active relation, copies the one
  certified candidate generation, then truncates staging in the same
  transaction.
- The daily business-intelligence guide now distinguishes temporary candidate
  staging growth from a post-certification capacity failure.
- Worker releases `m1-quote-staging-33e26bbf` and
  `m1-quote-truncate-active-de61e447` were pushed and installed individually
  on the Quote, coordinator and Structure Machines without changing their
  role-specific commands or concurrency settings.

## Verification

- Focused migration, Postgres promotion and transactional Quote worker tests:
  `28 passed`.
- Production migration reports `044 -> 045`; the staging relation exists.
- During a live candidate run, active held `69,956` published rows while
  staging grew independently (`4,000` then `69,956` rows). No candidate was
  visible to the business reader before certification.
- After certification, staging was observed at `0` and the Quote pointer
  advanced. A subsequent published active relation remained approximately
  `84MB`, instead of retaining the former `191MB` delete-bloat footprint.
- `make smoke-control-plane-prod` returned `200/available`; the business brief
  and the existing authenticated Dashboard session both rendered published
  M1 research data.

## Operational rule

Candidate staging may temporarily push capacity to `warning` or `critical`
while a generation is executing. It must be empty after its fenced
certification, and capacity must return below warning; otherwise treat it as a
storage incident rather than normal cadence.
