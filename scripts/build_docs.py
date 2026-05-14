"""Build the pdoc HTML reference site for Incorporator.

Reads the Google-style docstrings on every public symbol in the
``incorporator`` package and renders a static HTML site under ``./site/``.

Prerequisites:
    pip install -e ".[docs]"

Usage:
    python scripts/build_docs.py            # static build -> ./site/
    pdoc incorporator                       # live-reload dev server (port 8080)

Output is git-ignored (see ``site/`` in .gitignore). For a hosted reference,
publish ``./site/`` to GitHub Pages, Netlify, or S3.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import pdoc
except ImportError:
    sys.stderr.write(
        "pdoc is not installed. Install the docs extra:\n"
        '    pip install -e ".[docs]"\n'
    )
    sys.exit(1)


def main() -> None:
    output_dir = Path(__file__).resolve().parent.parent / "site"
    pdoc.render.configure(docformat="google", show_source=True)
    pdoc.pdoc("incorporator", output_directory=output_dir)
    print(f"Wrote pdoc HTML site to: {output_dir}")


if __name__ == "__main__":
    main()
