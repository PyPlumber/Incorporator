"""Block pytest collection of everything under ``examples/``.

Tutorials in ``examples/`` are runnable scripts, not test cases.  But pytest's
default collection rules match any function or method named ``test_*`` —
which means a tutorial function like ``test_demo()`` (a JIT-API-profiler
demo, no relation to pytest) would get collected and run as a test if
pytest were ever invoked with a working directory that includes
``examples/`` (e.g., ``pytest examples/`` or ``pytest --collect-only`` from
the repo root).

The repo-level ``[tool.pytest.ini_options].testpaths = ["tests"]`` setting
normally limits scope, but it's a soft fence — anyone overriding it on the
command line gets the footgun.  ``collect_ignore_glob = ["*"]`` is a hard
fence: pytest skips this whole directory and everything below it regardless
of how it was invoked.

If you ever want to add real tests for examples (smoke imports, etc.), put
them in ``tests/`` proper and keep this directory script-only.
"""

collect_ignore_glob = ["*"]
