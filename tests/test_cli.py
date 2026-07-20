"""Unit tests for the Incorporator Typer CLI."""

import json
from pathlib import Path
from typing import Any, AsyncGenerator, Iterator
from unittest.mock import patch

import pytest

pytest.importorskip("typer")
from typer.testing import CliRunner

from incorporator.cli import app
from incorporator.cli._pipeline_config import FjordConfig, StreamConfig
from incorporator.observability.logger import Wave

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_json_output_mode() -> Iterator[None]:
    """Reset the module-level ``_JSON_OUTPUT_MODE`` global after every test.

    The ``--json-output`` tests flip ``set_json_output_mode(True)`` and never
    restore it.  Under randomized ordering (pytest-randomly), a leaked ``True``
    routes ``_err`` output to STDERR, emptying ``result.stdout`` and breaking
    the error-path tests' substring assertions.  This autouse teardown keeps the
    global state isolated between tests regardless of execution order.
    """
    yield
    from incorporator.cli.runners import set_json_output_mode

    set_json_output_mode(False)


async def mock_stream(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    """Mocks the async generator to instantly yield 1 successful chunk."""
    yield Wave(chunk_index=1, rows_processed=500, failed_sources=[], processing_time_sec=1.5)


async def mock_stream_with_failures(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    """Mocks a stream that yields one chunk with failed sources."""
    yield Wave(
        chunk_index=1,
        rows_processed=10,
        failed_sources=["https://dead.example.com"],
        processing_time_sec=0.5,
    )


async def mock_stream_raises(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    """Mocks a stream that raises mid-yield to test the fatal-exception path."""
    raise RuntimeError("simulated network catastrophe")
    yield  # pragma: no cover (unreachable — keeps mypy happy about AsyncGenerator)


def test_cli_missing_config() -> None:
    """Ensures CLI fails gracefully if the JSON file is missing."""
    result = runner.invoke(app, ["stream", "non_existent.json"])
    assert result.exit_code == 1
    assert "Error: Configuration file not found" in result.stdout


def test_cli_version_flag_prints_version_and_exits_clean() -> None:
    """``incorporator --version`` prints the package version and exits 0.

    Senior-review pass-2 finding M-CLI2 — CI and ops scripts need a way to
    pin the running binary's version.  ``--version`` is the standard CLI
    hygiene affordance; the callback is registered on the root Typer app
    via ``is_eager=True`` so it short-circuits subcommand dispatch.
    """
    from incorporator import __version__

    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert f"incorporator {__version__}" in result.stdout


def test_cli_stream_success(tmp_path: Path) -> None:
    """Ensures the CLI correctly parses JSON and executes the stream bridge."""
    config_file = tmp_path / "pipeline.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://dummy.api"}}), encoding="utf-8")

    # Patch the real stream with our mock generator
    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream):
        result = runner.invoke(app, ["stream", str(config_file), "--poll", "60.0"])

        assert result.exit_code == 0
        assert "Starting Incorporator Stream" in result.stdout
        assert "Chunk 1" in result.stdout
        assert "500 rows" in result.stdout


# ==========================================
# ERROR-PATH COVERAGE
# ==========================================


def test_cli_invalid_json_config(tmp_path: Path) -> None:
    """A malformed JSON config must exit 1 with an 'Invalid JSON' error message."""
    config_file = tmp_path / "broken.json"
    config_file.write_text("{not: valid, json}", encoding="utf-8")

    result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    assert "Invalid JSON" in result.stdout


def test_cli_missing_incorp_params(tmp_path: Path) -> None:
    """A config with no 'incorp_params' key must exit 1 with a clear message."""
    config_file = tmp_path / "no_params.json"
    config_file.write_text(json.dumps({"refresh_params": {}}), encoding="utf-8")

    result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    # D2b: schema errors come from Pydantic now — substring match preserved.
    assert "incorp_params" in result.stdout


def test_cli_stream_reports_failed_sources(tmp_path: Path) -> None:
    """When the stream yields an Wave with failed_sources, the CLI must surface them."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream_with_failures):
        result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 0
    assert "Failures:" in result.stdout
    assert "dead.example.com" in result.stdout


def test_cli_keyboard_interrupt(tmp_path: Path) -> None:
    """KeyboardInterrupt raised inside asyncio.run must be caught; exit code stays 0."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.asyncio.run", side_effect=KeyboardInterrupt):
        result = runner.invoke(app, ["stream", str(config_file)])

    # The stream() handler catches KeyboardInterrupt and emits a message without sys.exit(1)
    assert result.exit_code == 0
    assert "stopped by user" in result.stdout


def test_cli_fatal_exception_in_stream(tmp_path: Path) -> None:
    """A mid-stream exception must exit 1 with the fatal-error banner."""
    config_file = tmp_path / "pipe.json"
    config_file.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream_raises):
        result = runner.invoke(app, ["stream", str(config_file)])

    assert result.exit_code == 1
    assert "Fatal Pipeline Error" in result.stdout
    assert "simulated network catastrophe" in result.stdout


# ==========================================
# FJORD SUBCOMMAND
# ==========================================

FJORD_USER_MODULE_SRC = """
from incorporator import Incorporator

class Coin(Incorporator):
    pass

class BinanceFutures(Incorporator):
    pass

def outflow(state):
    return [{"inc_code": "stub", "marker": "ok"}]
"""


def _write_fjord_fixture(tmp_path: Path) -> tuple[Path, Path]:
    """Write a user-module + pipeline.json pair into tmp_path. Returns (config, module).

    The user module is named ``coin_market.py`` so fjord auto-derives the
    output class name ``CoinMarket``.
    """
    user_module = tmp_path / "coin_market.py"
    user_module.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "outflow": "coin_market.py",
                "stream_params": [
                    {"cls_name": "Coin", "incorp_params": {"inc_url": "https://x"}},
                    {"cls_name": "BinanceFutures", "incorp_params": {"inc_url": "https://y"}},
                ],
                "export_params": {"file_path": str(tmp_path / "out.ndjson")},
            }
        ),
        encoding="utf-8",
    )
    return config, user_module


async def mock_fjord(*args: Any, **kwargs: Any) -> AsyncGenerator[Wave, None]:
    """Mocks the fjord async generator to yield two waves and exit."""
    yield Wave(chunk_index=1, operation="fjord_incorp:Coin", rows_processed=10, processing_time_sec=0.1)
    yield Wave(chunk_index=1, operation="outflow:CoinMarket", rows_processed=10, processing_time_sec=0.2)


def test_cli_fjord_success(tmp_path: Path) -> None:
    """fjord subcommand imports the outflow file, resolves source classes, and drains waves.

    The output class is auto-derived from the filename — no ``output_class``
    JSON key.
    """
    config, _ = _write_fjord_fixture(tmp_path)

    # Patch the LoggedIncorporator.fjord wrapper — the CLI now routes through
    # it so per-tick waves flow through the disk logger when --logs is set.
    with patch(
        "incorporator.cli.LoggedIncorporator.fjord",
        new=mock_fjord,
    ):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    assert "Starting Incorporator Fjord" in result.stdout
    assert "fjord_incorp:Coin" in result.stdout
    assert "outflow:CoinMarket" in result.stdout


def test_cli_fjord_missing_required_keys(tmp_path: Path) -> None:
    """fjord config missing required keys exits 1 with clear error."""
    config = tmp_path / "fjord.json"
    config.write_text(json.dumps({"outflow": "x.py"}), encoding="utf-8")

    result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 1
    # New validator reports each missing required key by name.
    assert "stream_params" in result.stdout
    assert "export_params" in result.stdout
    # output_class is no longer required — make sure the error message dropped it.
    assert "output_class" not in result.stdout


def test_cli_fjord_outflow_not_found(tmp_path: Path) -> None:
    """fjord config pointing at a non-existent outflow file exits 1."""
    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "outflow": "ghost.py",
                "stream_params": [{"cls_name": "Coin", "incorp_params": {}}],
                "export_params": {"file_path": "out.ndjson"},
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 1
    assert "outflow not found" in result.stdout


def test_cli_fjord_stream_missing_cls_name(tmp_path: Path) -> None:
    """stream_params entry missing cls_name exits 1."""
    user_module = tmp_path / "coin_market.py"
    user_module.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "outflow": "coin_market.py",
                "stream_params": [{"incorp_params": {}}],
                "export_params": {"file_path": "out.ndjson"},
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["fjord", str(config)])
    assert result.exit_code == 1
    assert "missing 'cls_name'" in result.stdout.lower() or "cls_name" in result.stdout


# ==========================================
# VALIDATE SUBCOMMAND
# ==========================================


def test_cli_validate_stream_ok(tmp_path: Path) -> None:
    """A well-formed stream config validates cleanly."""
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 0, result.stdout
    assert "is valid" in result.stdout


def test_cli_validate_stream_missing_source_key(tmp_path: Path) -> None:
    """incorp_params with no source key fails validation with a list of valid keys."""
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_code": "id"}}), encoding="utf-8")
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 1
    assert "at least one source key" in result.stdout


def test_cli_validate_fjord_ok(tmp_path: Path) -> None:
    """A well-formed fjord config (resolved outflow file, outflow arity OK) validates."""
    cfg, _ = _write_fjord_fixture(tmp_path)
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 0, result.stdout


def test_cli_validate_fjord_missing_outflow(tmp_path: Path) -> None:
    """outflow file without a top-level outflow() function fails fjord validation."""
    user_module = tmp_path / "broken_fjord.py"
    user_module.write_text(
        "from incorporator import Incorporator\nclass A(Incorporator): pass\n# no outflow() defined\n",
        encoding="utf-8",
    )
    cfg = tmp_path / "fjord.json"
    cfg.write_text(
        json.dumps(
            {
                "outflow": "broken_fjord.py",
                "stream_params": [{"cls_name": "A", "incorp_params": {"inc_url": "https://x"}}],
                "export_params": {"file_path": "out.ndjson"},
            }
        ),
        encoding="utf-8",
    )
    # autodetect picks fjord because of `outflow` + `stream_params` (list).
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 1
    assert "outflow" in result.stdout.lower()


def test_cli_validate_unset_env_var_reports_clearly(tmp_path: Path) -> None:
    """A required ${VAR} that isn't in the environment surfaces at validate-time."""
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(
        json.dumps(
            {
                "incorp_params": {
                    "inc_url": "https://x",
                    "headers": {"Authorization": "Bearer ${NONEXISTENT_TEST_VAR}"},
                }
            }
        ),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 1
    assert "NONEXISTENT_TEST_VAR" in result.stdout


def test_cli_validate_exits_0_on_cp1252_stdout(tmp_path: Path) -> None:
    """validate exits 0 and emits pure-ASCII output for a well-formed stream config.

    Proves that success output contains no non-ASCII characters, so it can never
    raise UnicodeEncodeError on a Windows cp1252 console.  The approach is
    "literals only" — all emoji/glyphs were removed from runtime output rather
    than translated, so the assertion is structural: every byte in result.stdout
    must round-trip through cp1252 encoding.
    """
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    result = runner.invoke(app, ["validate", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert "is valid" in result.stdout
    # Pure-ASCII: no character above U+007F survives in the success message.
    assert result.stdout.isascii()
    # Belt-and-suspenders: encoding to cp1252 must not raise UnicodeEncodeError.
    result.stdout.encode("cp1252")


# ==========================================
# INIT SUBCOMMAND
# ==========================================


def test_cli_init_stream_writes_pipeline_json(tmp_path: Path) -> None:
    """init --type stream writes a single pipeline.json scaffold."""
    result = runner.invoke(app, ["init", "--output-dir", str(tmp_path), "--type", "stream"])
    assert result.exit_code == 0, result.stdout
    assert (tmp_path / "pipeline.json").is_file()
    assert "Wrote 1 starter file" in result.stdout


def test_cli_init_fjord_writes_two_files(tmp_path: Path) -> None:
    """init --type fjord writes pipeline.json + outflow.py."""
    result = runner.invoke(app, ["init", "--output-dir", str(tmp_path), "--type", "fjord"])
    assert result.exit_code == 0, result.stdout
    assert (tmp_path / "pipeline.json").is_file()
    assert (tmp_path / "outflow.py").is_file()


def test_cli_init_refuses_overwrite(tmp_path: Path) -> None:
    """init must refuse to clobber an existing pipeline.json."""
    (tmp_path / "pipeline.json").write_text("{}", encoding="utf-8")
    result = runner.invoke(app, ["init", "--output-dir", str(tmp_path), "--type", "stream"])
    assert result.exit_code == 1
    assert "Refusing to overwrite" in result.stdout


# ==========================================
# --json-output FLAG
# ==========================================


def test_cli_stream_json_output_emits_ndjson(tmp_path: Path) -> None:
    """--json-output emits one NDJSON Wave line per chunk.

    Under the installed Click version (8.2+), ``CliRunner`` always captures
    stdout and stderr independently, so ``result.stdout`` is already pure
    NDJSON here — the line filter below is retained for robustness rather
    than necessity. See ``test_cli_stream_json_output_stdout_is_pure_ndjson``
    for the pin that asserts the stdout/stderr split directly.
    """
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream):
        result = runner.invoke(app, ["stream", str(cfg), "--json-output"])

    assert result.exit_code == 0, result.stdout
    # Filter for JSON-looking lines and confirm they parse with the wave shape.
    json_lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert json_lines, f"expected at least one NDJSON wave line, got: {result.stdout!r}"
    for line in json_lines:
        record = json.loads(line)
        assert "rows_processed" in record
        assert "chunk_index" in record
        assert "operation" in record
        # New Wave fields from session schema enrichments (commits c77d60d, 0bc9e9a)
        assert "source_url" in record
        assert "bytes_processed" in record
        assert "http_retry_count" in record
        assert "validation_error_count" in record
        assert "schema_cache_hit" in record
        assert "conv_dict_time_sec" in record
        # Type checks (defaults: None or 0)
        assert isinstance(record["http_retry_count"], int)
        assert isinstance(record["validation_error_count"], int)
        assert isinstance(record["schema_cache_hit"], bool)


def test_cli_fjord_json_output_emits_ndjson(tmp_path: Path) -> None:
    """--json-output emits one NDJSON Wave line per fjord wave, including new enrichment fields.

    Parallel to test_cli_stream_json_output_emits_ndjson but exercises the
    fjord subcommand path.  All Wave fields added in the schema enrichment
    commits (source_url, bytes_processed, http_retry_count,
    validation_error_count, schema_cache_hit, conv_dict_time_sec) must be
    present in the serialised NDJSON output.
    """
    config, _ = _write_fjord_fixture(tmp_path)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=mock_fjord):
        result = runner.invoke(app, ["fjord", str(config), "--json-output"])

    assert result.exit_code == 0, result.stdout
    json_lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    assert json_lines, f"expected at least one NDJSON wave line, got: {result.stdout!r}"
    for line in json_lines:
        record = json.loads(line)
        assert "rows_processed" in record
        assert "chunk_index" in record
        assert "operation" in record
        assert "source_url" in record
        assert "bytes_processed" in record
        assert "http_retry_count" in record
        assert "validation_error_count" in record
        assert "schema_cache_hit" in record
        assert "conv_dict_time_sec" in record
        assert isinstance(record["http_retry_count"], int)
        assert isinstance(record["validation_error_count"], int)
        assert isinstance(record["schema_cache_hit"], bool)


def test_cli_stream_json_output_stdout_is_pure_ndjson(tmp_path: Path) -> None:
    """--json-output routes the startup banner to stderr, leaving stdout pure NDJSON.

    Platform-review Stage 0 pin: under Click 8.2+ (confirmed installed here),
    ``CliRunner.invoke`` always captures stdout and stderr independently —
    ``result.stdout`` is real stdout, not the interleaved mix. ``_err(...)``
    (incorporator/cli/runners.py) routes to stderr exactly when
    ``set_json_output_mode(True)`` has been called, which the ``stream``
    command does before invoking ``_run_stream``. Every non-blank stdout
    line must therefore be a parseable JSON object, and the banner text
    must appear on stderr instead.
    """
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream):
        result = runner.invoke(app, ["stream", str(cfg), "--json-output"])

    assert result.exit_code == 0, result.stdout
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert lines, f"expected at least one NDJSON line on stdout; got: {result.stdout!r}"
    for line in lines:
        assert line.startswith("{"), f"stdout must carry ONLY NDJSON under --json-output; got line: {line!r}"
        json.loads(line)
    assert "Starting Incorporator Stream" in result.stderr


def test_cli_fjord_json_output_stdout_is_pure_ndjson(tmp_path: Path) -> None:
    """--json-output routes the startup banner to stderr for `fjord`, mirroring the stream pin.

    See ``test_cli_stream_json_output_stdout_is_pure_ndjson`` for the
    mechanism (``_err`` + ``set_json_output_mode``); this pin exercises the
    ``fjord`` command's identical wiring in ``cli/__init__.py``.
    """
    config, _ = _write_fjord_fixture(tmp_path)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=mock_fjord):
        result = runner.invoke(app, ["fjord", str(config), "--json-output"])

    assert result.exit_code == 0, result.stdout
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert lines, f"expected at least one NDJSON line on stdout; got: {result.stdout!r}"
    for line in lines:
        assert line.startswith("{"), f"stdout must carry ONLY NDJSON under --json-output; got line: {line!r}"
        json.loads(line)
    assert "Starting Incorporator Fjord" in result.stderr


# ==========================================
# HEARTBEAT FILE
# ==========================================


def test_cli_stream_heartbeat_file_touched(tmp_path: Path) -> None:
    """--heartbeat-file causes the CLI to touch the path after every wave."""
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps({"incorp_params": {"inc_url": "https://x"}}), encoding="utf-8")
    heartbeat = tmp_path / "hb.beat"
    assert not heartbeat.exists()

    with patch("incorporator.cli.LoggedIncorporator.stream", new=mock_stream):
        result = runner.invoke(
            app,
            ["stream", str(cfg), "--heartbeat-file", str(heartbeat)],
        )

    assert result.exit_code == 0, result.stdout
    assert heartbeat.exists()


# ==========================================
# Finding 1 — inflow/outflow forwarded to stream()
# ==========================================


def test_cli_stream_forwards_inflow_and_outflow_to_stream(tmp_path: Path) -> None:
    """stream runner forwards 'inflow' and 'outflow' config keys to LoggedIncorporator.stream().

    A stateful_polling=True pipeline that declares 'outflow' in the JSON config
    must have outflow= present in the kwargs passed to stream().  Before this
    fix, both keys were silently discarded — the outflow sidecar class was never
    loaded by the stateful engine even though the config declared it.
    """
    inflow_file = tmp_path / "inflow.py"
    inflow_file.write_text("# inflow stub\n", encoding="utf-8")
    outflow_file = tmp_path / "outflow.py"
    outflow_file.write_text(
        "from incorporator import Incorporator\nclass Out(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
        encoding="utf-8",
    )

    cfg = tmp_path / "pipeline.json"
    cfg.write_text(
        json.dumps(
            {
                "incorp_params": {"inc_url": "https://x"},
                "stateful_polling": True,
                "inflow": "inflow.py",
                "outflow": "outflow.py",
            }
        ),
        encoding="utf-8",
    )

    captured_kwargs: dict[str, Any] = {}

    async def _capture_stream(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=1, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.stream", new=_capture_stream):
        result = runner.invoke(app, ["stream", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert "inflow" in captured_kwargs, "inflow must be forwarded to stream()"
    assert "outflow" in captured_kwargs, "outflow must be forwarded to stream()"
    # After resolve_config_paths, inflow/outflow are config-dir-absolute paths.
    assert str(inflow_file.resolve()) == captured_kwargs["inflow"]
    assert str(outflow_file.resolve()) == captured_kwargs["outflow"]


def test_cli_stream_inflow_outflow_absent_stays_none(tmp_path: Path) -> None:
    """When 'inflow'/'outflow' are absent from the config, stream() receives None for both.

    The absence case must be explicitly forwarded as None= rather than omitted,
    so stream() sees its default parameter value rather than whatever was left
    from a previous call.  This guards against the None/absent distinction in
    any future stream() overload.
    """
    cfg = tmp_path / "pipeline.json"
    cfg.write_text(
        json.dumps({"incorp_params": {"inc_url": "https://x"}}),
        encoding="utf-8",
    )

    captured_kwargs: dict[str, Any] = {}

    async def _capture_stream(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.stream", new=_capture_stream):
        result = runner.invoke(app, ["stream", str(cfg)])

    assert result.exit_code == 0, result.stdout
    assert captured_kwargs.get("inflow") is None
    assert captured_kwargs.get("outflow") is None


# ==========================================
# Fjord CLI parity bugs — inflow forwarding + sidecar sys.modules cache key
# ==========================================


def test_cli_fjord_forwards_inflow_to_fjord(tmp_path: Path) -> None:
    """fjord runner forwards the 'inflow' config key to LoggedIncorporator.fjord().

    Before this fix, ``_run_fjord`` never read ``config.get("inflow")`` and
    never passed ``inflow=`` to the engine call — a fjord config declaring
    ``"inflow": "inflow.py"`` validated cleanly but had it silently discarded
    at run time.
    """
    inflow_file = tmp_path / "inflow.py"
    inflow_file.write_text("# inflow stub\n", encoding="utf-8")
    outflow_file = tmp_path / "coin_market.py"
    outflow_file.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "inflow": "inflow.py",
                "outflow": "coin_market.py",
                "stream_params": [{"cls_name": "Coin", "incorp_params": {"inc_url": "https://x"}}],
                "export_params": {"file_path": str(tmp_path / "out.ndjson")},
            }
        ),
        encoding="utf-8",
    )

    captured_kwargs: dict[str, Any] = {}

    async def _capture_fjord(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=_capture_fjord):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    assert "inflow" in captured_kwargs, "inflow must be forwarded to fjord()"
    assert captured_kwargs["inflow"] == str(inflow_file.resolve())


def test_cli_fjord_inflow_absent_stays_none(tmp_path: Path) -> None:
    """When 'inflow' is absent from a fjord config, fjord() receives None.

    Guards the None/absent distinction explicitly, mirroring the equivalent
    stream() coverage.
    """
    config, _ = _write_fjord_fixture(tmp_path)

    captured_kwargs: dict[str, Any] = {}

    async def _capture_fjord(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=_capture_fjord):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    assert captured_kwargs.get("inflow") is None


def test_cli_fjord_resolves_cls_name_against_same_module_as_token_resolver(tmp_path: Path) -> None:
    """The class bound to a token-resolved conv_dict callable must be the SAME
    object the fjord runner resolves 'cls_name' against.

    Root cause under audit: ``_load_pipeline_config`` loads ``outflow.py`` once
    (via ``merge_sidecar_extra_names``, default ``sys.modules`` hint) to build
    the token-resolver allow-list, then ``_run_fjord``'s own loader used a
    DIFFERENT hint (``_inc_fjord_user_module``) for the same path — producing
    two independently-``exec``'d module objects.  A helper referenced from a
    JSON token (e.g. ``"@identity_conv"``) would then close over its own
    module's ``Coin`` class, which is not the ``Coin`` object seeded into
    ``stream_params[i]["cls"]``.  This test fails on unpatched ``main`` and
    passes once both loads share the default ``sys.modules`` cache key.
    """
    outflow_file = tmp_path / "outflow.py"
    outflow_file.write_text(
        "from incorporator import Incorporator\n"
        "class Coin(Incorporator):\n    pass\n"
        "def identity_conv(x):\n    return x\n"
        "def outflow(state):\n    return []\n",
        encoding="utf-8",
    )

    config = tmp_path / "fjord.json"
    config.write_text(
        json.dumps(
            {
                "outflow": "outflow.py",
                "stream_params": [
                    {
                        "cls_name": "Coin",
                        "incorp_params": {"inc_url": "https://x", "conv_dict": {"foo": "@identity_conv"}},
                    }
                ],
                "export_params": {"file_path": str(tmp_path / "out.ndjson")},
            }
        ),
        encoding="utf-8",
    )

    captured_kwargs: dict[str, Any] = {}

    async def _capture_fjord(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=_capture_fjord):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    resolved_cls = captured_kwargs["stream_params"][0]["cls"]
    conv_fn = captured_kwargs["stream_params"][0]["incorp_params"]["conv_dict"]["foo"]
    assert conv_fn.__globals__["Coin"] is resolved_cls


# ==========================================
# Structural regression guard — every StreamConfig / FjordConfig field must
# reach the engine call, or be explicitly exempt.
#
# The recurring bug class (see AGENTS.md, commits 24b65bd + 25627d9): a
# config schema accepts a key, but the runner that consumes it never
# forwards the value — ``incorporator validate`` passes, the key is
# silently dropped at runtime.  Two prior fixes were each hand-written
# per-key regression tests (test_cli_stream_forwards_inflow_and_outflow_to_stream,
# test_cli_fjord_forwards_inflow_to_fjord) that could only catch a
# recurrence of the SAME field on the SAME verb.  These tests instead
# derive their expectations from ``StreamConfig``/``FjordConfig.model_fields``,
# so a newly added field is covered — forwarded or explicitly exempted — the
# day it is added, not the day someone happens to hand-write a matching test.
# ==========================================

_STREAM_EXEMPT_FIELDS = {
    "poll_interval": "driven by the --poll CLI flag (see docs/cli_and_configuration.md "
    "'Daemon Execution'), never read from config",
}

_STREAM_FIELD_SENTINELS: dict[str, Any] = {
    "incorp_params": {"inc_url": "https://x", "_sentinel": "stream_incorp"},
    "refresh_params": {"_sentinel": "stream_refresh"},
    "export_params": {"_sentinel": "stream_export"},
    "refresh_interval": 111.5,
    "export_interval": 222.5,
    "stateful_polling": True,
    "inflow": "inflow.py",
    "outflow": "outflow.py",
}


def test_stream_config_field_coverage_is_exhaustive() -> None:
    """Every StreamConfig field is either sentinel-covered or explicitly exempt.

    Fails the day a new field is added to StreamConfig without a matching
    decision here — the property that would have caught the inflow/outflow
    forwarding bug the day it was introduced, rather than weeks later.
    """
    covered = set(_STREAM_FIELD_SENTINELS) | set(_STREAM_EXEMPT_FIELDS)
    uncovered = set(StreamConfig.model_fields) - covered
    assert not uncovered, (
        f"StreamConfig field(s) {sorted(uncovered)} are neither sentinel-covered nor "
        "explicitly exempt above — add a sentinel or a commented exemption."
    )


def test_stream_config_kitchen_sink_forwards_every_field(tmp_path: Path) -> None:
    """Every non-exempt StreamConfig field's sentinel value reaches ``LoggedIncorporator.stream()``.

    Builds one config declaring every non-exempt field with a recognisable
    sentinel value, then asserts each sentinel arrives unchanged in the
    captured kwargs. A field that ``_run_stream`` stops forwarding fails
    here immediately, regardless of which field it is.
    """
    inflow_file = tmp_path / "inflow.py"
    inflow_file.write_text("# inflow stub\n", encoding="utf-8")
    outflow_file = tmp_path / "outflow.py"
    outflow_file.write_text(
        "from incorporator import Incorporator\nclass Out(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n",
        encoding="utf-8",
    )

    cfg = tmp_path / "pipeline.json"
    cfg.write_text(json.dumps(_STREAM_FIELD_SENTINELS), encoding="utf-8")

    captured_kwargs: dict[str, Any] = {}

    async def _capture_stream(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=1, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.stream", new=_capture_stream):
        result = runner.invoke(app, ["stream", str(cfg)])

    assert result.exit_code == 0, result.stdout
    for field, sentinel in _STREAM_FIELD_SENTINELS.items():
        assert field in captured_kwargs, f"{field!r} must be forwarded to stream()"
        if field in ("inflow", "outflow"):
            # After resolve_config_paths, inflow/outflow are config-dir-absolute.
            expected: Any = str((tmp_path / sentinel).resolve())
        else:
            expected = sentinel
        assert captured_kwargs[field] == expected, f"{field}: expected {expected!r}, got {captured_kwargs[field]!r}"


_FJORD_EXEMPT_FIELDS = {
    "stream_params": (
        "restructured per-entry (cls_name resolved -> cls) before forwarding; "
        "already covered by test_cli_fjord_success and "
        "test_cli_fjord_resolves_cls_name_against_same_module_as_token_resolver"
    ),
}

_FJORD_FIELD_SENTINELS: dict[str, Any] = {
    "outflow": "coin_market.py",
    "inflow": "inflow.py",
    "export_params": {"file_path": "out.ndjson", "_sentinel": "fjord_export"},
    "refresh_interval": 333.5,
    "export_interval": 444.5,
}


def test_fjord_config_field_coverage_is_exhaustive() -> None:
    """Every FjordConfig field is either sentinel-covered or explicitly exempt.

    Same guard as ``test_stream_config_field_coverage_is_exhaustive``, for
    the fjord verb — the sibling that stayed broken ~40 days after the
    stream fix (commit 25627d9) precisely because no such structural
    check existed.
    """
    covered = set(_FJORD_FIELD_SENTINELS) | set(_FJORD_EXEMPT_FIELDS)
    uncovered = set(FjordConfig.model_fields) - covered
    assert not uncovered, (
        f"FjordConfig field(s) {sorted(uncovered)} are neither sentinel-covered nor "
        "explicitly exempt above — add a sentinel or a commented exemption."
    )


def test_fjord_config_kitchen_sink_forwards_every_field(tmp_path: Path) -> None:
    """Every non-exempt FjordConfig field's sentinel value reaches ``LoggedIncorporator.fjord()``.

    Parallel to ``test_stream_config_kitchen_sink_forwards_every_field``.
    ``stream_params`` is intentionally excluded from the loop (see
    ``_FJORD_EXEMPT_FIELDS``) and supplied separately so the config
    validates.
    """
    inflow_file = tmp_path / "inflow.py"
    inflow_file.write_text("# inflow stub\n", encoding="utf-8")
    outflow_file = tmp_path / "coin_market.py"
    outflow_file.write_text(FJORD_USER_MODULE_SRC, encoding="utf-8")

    config_dict: dict[str, Any] = dict(_FJORD_FIELD_SENTINELS)
    config_dict["export_params"] = {
        **_FJORD_FIELD_SENTINELS["export_params"],
        "file_path": str(tmp_path / "out.ndjson"),
    }
    config_dict["stream_params"] = [{"cls_name": "Coin", "incorp_params": {"inc_url": "https://x"}}]

    config = tmp_path / "fjord.json"
    config.write_text(json.dumps(config_dict), encoding="utf-8")

    captured_kwargs: dict[str, Any] = {}

    async def _capture_fjord(**kwargs: Any) -> Any:  # type: ignore[return]
        captured_kwargs.update(kwargs)
        yield Wave(chunk_index=1, rows_processed=0, processing_time_sec=0.0)

    with patch("incorporator.cli.LoggedIncorporator.fjord", new=_capture_fjord):
        result = runner.invoke(app, ["fjord", str(config)])

    assert result.exit_code == 0, result.stdout
    for field, sentinel in _FJORD_FIELD_SENTINELS.items():
        assert field in captured_kwargs, f"{field!r} must be forwarded to fjord()"
        if field == "outflow":
            expected: Any = outflow_file.resolve()
        elif field == "inflow":
            expected = str(inflow_file.resolve())
        elif field == "export_params":
            expected = {**sentinel, "file_path": str(tmp_path / "out.ndjson")}
        else:
            expected = sentinel
        assert captured_kwargs[field] == expected, f"{field}: expected {expected!r}, got {captured_kwargs[field]!r}"
