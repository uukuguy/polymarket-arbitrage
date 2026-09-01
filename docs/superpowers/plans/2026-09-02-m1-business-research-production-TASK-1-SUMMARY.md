# M1 Business Research Production — Task 1 Summary

**Delivered:** Structure research pages now select the same latest certified
Structure manifest used by the atomic business overview. They no longer depend
on a `structure:current` pointer, which production deliberately does not write.

**Why:** The dashboard could show a valid Structure generation in the overview
while its detailed page returned `structure-not-published`. That contradicted
the sole authority and hid the very base data the business view is intended to
make inspectable.

**Verification:** A real-Postgres regression fixture omits any Structure
pointer, provides two certified manifests, and proves only the newest
generation's rows appear. The focused Postgres and API route contracts pass.
