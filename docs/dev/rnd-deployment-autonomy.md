# R&D Deployment Autonomy

## Default

For initial and ongoing R&D, the agent deploys a locally verified commit
directly and records the exact SHA, effective configuration, Fly release, and
post-deploy evidence. `DEPLOY_SHA_APPROVE` must not be used as a blocking
request for a SHA the agent just generated.

This keeps the research loop continuous without weakening traceability: the
deployment record, not a user echo of the same identifier, is the audit trail.

## Human approval boundaries

Explicit user approval is still required before a deployment or operation
would change any of the following:

- funds, balances, transfer permissions, or order placement;
- secrets or credentials;
- Structure read mode, serving-pointer overrides, or manual production-data
  mutation;
- enabling Quote workers;
- disabling cleanup/retention protections;
- another irreversible production-state change.

Routine code fixes, observability changes, and self-healing repairs that keep
those protections unchanged proceed autonomously after local verification.
