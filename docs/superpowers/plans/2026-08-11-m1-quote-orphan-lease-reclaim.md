# Quote supervisor orphan-lease reclaim

## Production fact

During a rolling deployment, the old supervised Quote child can be terminated
without running its normal lease release. Its SQLite lease remains valid for
the 210-second Quote supervisor timeout. The replacement child then waits for
expiry before it can collect any quotes, needlessly making the opportunity API
stale and unavailable.

## Narrow recovery rule

At startup, the replacement **supervised Quote child only** atomically removes
a live `owner='quote'` lease and writes the ordinary `released` receipt. It
cannot reclaim a Structure lease. This is safe in the production topology: the
single Quote supervisor starts a replacement only after terminating its prior
child.

## Evidence required

1. Unit test proves same-owner lease is released and immediately acquirable.
2. Unit test proves a Structure lease survives the Quote-only action.
3. Worker-entry test proves reclaim happens before Quote worker construction.
4. Production release records a `released` receipt / recovery log and a fresh
   Quote success, without waiting for the former 210-second expiry.
