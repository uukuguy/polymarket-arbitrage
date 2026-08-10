# M1 Supervisor Diagnostic Receipt

**Goal:** Make a Quote supervisor spawn/control failure diagnosable in the
Fly-native incident console without SSH, secrets, or an unbounded history read.

**Design:** Append only a stable exception type to the bounded, redacted child
stderr receipt on supervisor failures. Add a `LIMIT 1` store projection and
render the latest Quote supervisor receipt beside the durable Quote checkpoint.

**Security:** Never store the exception message; it can contain a path, token,
or upstream URL. The existing receipt sanitizer remains the final boundary.

**Verification:** A RED test injects an `OSError` containing a token-like
message and proves that the receipt contains only
`supervisor-spawn-error:OSError`. HTTP tests prove no phantom receipt and the
exact latest durable receipt. Run focused supervision/console tests and Ruff.
