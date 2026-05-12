"""Performance benchmark suite for Incorporator.

Tests in this package are auto-marked ``@pytest.mark.benchmark`` by conftest.py
and excluded from the default ``pytest tests/`` run via the ``-m 'not benchmark'``
filter in pyproject.toml. Run them explicitly with::

    pytest tests/benchmarks/ -m benchmark -v
"""
