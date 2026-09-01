# M1 Structure Pool Pressure — Task 1 Summary

**Delivered:** The deployed Structure normalization worker now caps its
concurrent fenced range lanes at two, matching its two-session Postgres pool.

**Why:** Structure range completion writes both durable receipts and the
generation-bound business-research index. Its prior default of twelve lanes
would create the same self-inflicted pool contention observed in Quote.

**Verification:** The deployment-template contract first failed when the
Structure limit was absent; the template and rollout suites now pass with both
Quote and Structure lane budgets aligned to the database envelope.
