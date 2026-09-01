# M1 Quote certification publish window — Task 1 Summary

## Outcome

Quote certification has a lease long enough to publish a complete large
generation and prune its superseded dashboard index in one fenced transaction.
This removes the failure mode where a valid 70k-row candidate became stranded
after the old 30-second certification lease expired.

## Implementation

- The transactional Quote factory now constructs `TransactionalQuoteCertifier`
  with a 120-second lease.
- Quote batch I/O, admission, and normal runtime deadlines are unchanged.
- The longer lease applies only to the pointer-publication transaction, whose
  work includes validating all receipts and removing the prior dashboard index.

## Verification

- RED: factory contract required the 120-second publish lease and failed.
- GREEN: focused factory tests prove R2 retry bounds are unchanged and the
  certifier receives the 120-second lease.

## Production follow-up

Deploy this coordinator image, then allow the already-complete candidate Quote
generation to certify. Verify the pointer advances and only the new generation
remains before re-enabling normal Quote refresh cadence.
