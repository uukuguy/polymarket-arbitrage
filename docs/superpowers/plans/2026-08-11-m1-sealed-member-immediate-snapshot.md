# Immediate fresh snapshot after member sealing

When a non-deferred event-member sidecar seals, retain scheduler checkpoint
continuation so the next bounded tick can admit a fresh Structure snapshot.
This converts sealed source evidence into post-failure certified publication
without waiting the ordinary cadence and without bypassing Quote arbitration.
