from __future__ import annotations

import pytest

from polyarb.control_plane.models import StructureSourcePageSpec


def test_structure_source_page_spec_keeps_opaque_cursor_in_stable_job_identity() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:20260812-1",
        stream="events",
        ordinal=3,
        requested_cursor="opaque:next/page",
    )

    assert spec.job_key == "source-window:20260812-1:fetch:events:3"
    assert spec.input_identity == (
        "source-window:20260812-1:events:3:opaque:next/page"
    )


@pytest.mark.parametrize(
    ("stream", "ordinal"),
    (("unknown", 0), ("events", -1)),
)
def test_structure_source_page_spec_rejects_invalid_stream_or_ordinal(
    stream: str, ordinal: int
) -> None:
    with pytest.raises(ValueError):
        StructureSourcePageSpec(
            window_key="source-window:20260812-1",
            stream=stream,
            ordinal=ordinal,
            requested_cursor=None,
        )
