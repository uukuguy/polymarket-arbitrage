# M1 Quote Supervisor Console Scope — Task 7 Summary

## Delivered

The direct Fly incident console now queries recovered Quote supervisor
histories with durable scope `quote`, matching `ProducerSupervisor`'s actual
incident scope. It no longer queries the nonexistent `producer:quote` scope,
which caused closed supervisor recovery evidence to be omitted from the
operator surface.

## Verification

The console contract was changed red-first, then passed:

`uv run pytest tests/m1-perception/test_perception_http.py::test_perception_console_is_a_direct_operator_view -q`

Ruff and changed-path diff checks also passed.
