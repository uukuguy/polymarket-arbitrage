"""Top-level CLI entry point.

The ``[project.scripts] polyarb = "polyarb.cli:app"`` declaration in
``pyproject.toml`` (Plan 1) points here. We re-export the typer app from
``polyarb.snapshot.cli`` so the console script and ``python -m polyarb.snapshot``
both resolve to the same Typer instance.

Future subcommands (e.g. ``scan-arb``, ``watch-orderbook`` from later phases)
will be registered on this same ``app`` via ``app.command()`` decorators in
their own modules, imported here.
"""

from polyarb.snapshot.cli import app

__all__ = ["app"]
