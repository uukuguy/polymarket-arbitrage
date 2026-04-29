"""Entry for ``python -m polyarb.snapshot``.

Matches CONTEXT.md MK1/MK2 invocation form. The Makefile uses this path.
"""

from polyarb.snapshot.cli import app

if __name__ == "__main__":
    app()
