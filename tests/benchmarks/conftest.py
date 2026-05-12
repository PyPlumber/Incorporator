"""Auto-mark every test in tests/benchmarks/ with the 'benchmark' pytest marker.

This lets the default ``pytest tests/`` invocation (configured with
``addopts = -m 'not benchmark'``) skip the suite entirely so the standard
unit-test loop stays fast, while ``pytest tests/benchmarks/ -m benchmark``
runs only the perf tests.
"""

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list) -> None:
    """Tag every collected item under tests/benchmarks/ with the benchmark marker."""
    benchmark_mark = pytest.mark.benchmark
    for item in items:
        if "benchmarks" in str(item.fspath):
            item.add_marker(benchmark_mark)
