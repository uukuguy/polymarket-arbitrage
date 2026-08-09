# M1 Isolated Cleanup Owner Repair Summary

Production health correctly reported a stale enabled cleanup runtime: the
runtime record was durable, but no worker existed to advance it under isolated
producers. The parent daemon now runs the bounded, fail-soft cleanup owner in
that topology. It remains disabled only when Structure synchronization or the
cleanup feature is disabled.
