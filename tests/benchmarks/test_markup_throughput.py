"""Benchmark: prove XML and HTML markup-format throughput.

XML is bidirectional (parse + write); HTML is **parse-only** by design (see
``incorporator/io/handlers/markup.py`` — writing HTML tables is intentionally
unsupported because it conflicts with the framework's structured-data focus).
So this file benchmarks XML write throughput and HTML parse throughput.

Markup serialisation/parsing is inherently slower than delimited or columnar
formats — every row produces a nested element tree with escaping and
indentation overhead.  Floors here are deliberately lower than the
delimited/JSON benches.

ROW_COUNT is smaller than the columnar benches (100k vs 500k) because markup
serialisation is CPU-bound and would dominate the full benchmark suite at
500k rows.  100k is large enough that one-time startup overhead averages out.
"""

import time
from pathlib import Path
from typing import Iterable

import pytest

from incorporator.io.formats import FormatType
from incorporator.io.handlers import parse_source_data, write_destination_data

ROW_COUNT = 100_000  # smaller than delimited benches — markup serialisation is slow


def _generate_rows() -> Iterable[dict]:
    for i in range(ROW_COUNT):
        yield {"id": i, "name": f"row_{i}", "value": i * 1.5, "active": bool(i % 2)}


@pytest.mark.asyncio
async def test_xml_streaming_throughput(tmp_path: Path) -> None:
    """XML streaming write must sustain at least 20k rows/sec.

    XML is element-tree based — every row is a nested record with opening
    and closing tags plus attribute escaping.  A 20k floor is realistic for
    pure Python stdlib ``xml.etree`` serialisation.
    """
    out_path = tmp_path / "stream.xml"

    t0 = time.perf_counter()
    await write_destination_data(_generate_rows(), out_path, FormatType.XML)
    elapsed = time.perf_counter() - t0

    throughput = ROW_COUNT / elapsed
    print(f"\n  XML streaming write: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # Sanity: file exists, contains an opening XML element.
    assert out_path.exists()
    head = out_path.read_text(encoding="utf-8")[:200]
    assert "<" in head, "XML output is missing element markup."

    # 20k floor — XML element-tree serialisation is fundamentally slower than
    # delimited formats but should comfortably hit 20k/sec even on CI hardware.
    assert throughput >= 20_000, (
        f"XML throughput {throughput:,.0f} rows/sec is below 20k floor. "
        "Investigate whether xml.etree is being used (lxml is faster but is an opt-in dep)."
    )


@pytest.mark.asyncio
async def test_html_parse_throughput(tmp_path: Path) -> None:
    """HTML parse must sustain at least 10k rows/sec.

    HTMLHandler is parse-only by design (see module docstring at
    ``incorporator/io/handlers/markup.py``).  We synthesise a single
    ``<table>`` with ROW_COUNT rows, then measure ``parse_source_data``
    end-to-end.

    Floor is set conservatively at 10k/sec.  Measured baseline with the
    stdlib ``html.parser`` is ~18k/sec; with ``lxml`` (an opt-in extra
    via ``pip install incorporator[speedups]``) it typically jumps 2–3×.
    """
    # Build a synthetic HTML table once — the bench measures parse, not write.
    rows_html = "\n".join(
        f"<tr><td>{i}</td><td>row_{i}</td><td>{i * 1.5}</td><td>{bool(i % 2)}</td></tr>" for i in range(ROW_COUNT)
    )
    html_doc = (
        "<html><body><table>"
        "<tr><th>id</th><th>name</th><th>value</th><th>active</th></tr>"
        f"{rows_html}"
        "</table></body></html>"
    )
    src_path = tmp_path / "synth.html"
    src_path.write_text(html_doc, encoding="utf-8")

    t0 = time.perf_counter()
    parsed = await parse_source_data(src_path, FormatType.HTML)
    elapsed = time.perf_counter() - t0

    # parse_source_data returns Union[dict, list[dict]] — HTML always returns a list.
    assert isinstance(parsed, list)
    assert len(parsed) == ROW_COUNT, f"Expected {ROW_COUNT} rows, got {len(parsed)}"

    throughput = ROW_COUNT / elapsed
    print(f"\n  HTML parse: {throughput:,.0f} rows/sec ({elapsed:.2f}s)")

    # 10k floor — accommodates stdlib html.parser baseline.  Below 10k
    # suggests we lost the streaming row extraction and are now allocating
    # a full intermediate structure before yielding dicts.
    assert throughput >= 10_000, (
        f"HTML parse throughput {throughput:,.0f} rows/sec is below 10k floor. "
        "Suggests we're materialising the full DOM tree before row extraction."
    )
