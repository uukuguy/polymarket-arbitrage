# M1 Quote Supervised Recovery — Task 5 Summary

## Delivered

The direct Fly incident console now includes recovered `producer:quote`
histories beside Quote-collection and capacity incidents. A bounded recovery
or an exhausted restart budget is therefore retained on the operator surface
after the live card closes; it is not visible only while open.

## Verification

`uv run pytest tests/m1-perception/test_perception_http.py::test_perception_console_is_a_direct_operator_view -q`

Result: passed, with Ruff and diff checks clean.
