"""Validation and correctness tests for new guardrails added in the remediation phases."""

import asyncio
import json
import textwrap
from pathlib import Path
from typing import List

import pytest

from incorporator import Incorporator


# ==========================================
# 1. export() ISINSTANCE GUARD
# ==========================================


@pytest.mark.asyncio
async def test_export_rejects_invalid_instance_type(tmp_path: Path) -> None:
    """export() must raise TypeError when instance is not a list/Incorporator/BaseModel."""

    class ExportGuardModel(Incorporator):
        pass

    with pytest.raises(TypeError, match="instance"):
        await ExportGuardModel.export(
            instance="this_is_a_plain_string",  # type: ignore[arg-type]
            file_path=str(tmp_path / "out.csv"),
        )


@pytest.mark.asyncio
async def test_export_accepts_list_instance(tmp_path: Path) -> None:
    """export() must NOT raise when instance is a properly populated list."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    class ExportListModel(Incorporator):
        pass

    result = await ExportListModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_path = tmp_path / "out.json"

    # Must not raise
    await ExportListModel.export(instance=result, file_path=str(out_path))
    assert out_path.exists()


# ==========================================
# 2. transform() SIGNATURE VALIDATION
# ==========================================


@pytest.mark.asyncio
async def test_code_file_transform_wrong_arity_raises(tmp_path: Path) -> None:
    """_apply_code_transform must raise ValueError when transform has wrong arity."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1}]), encoding="utf-8")

    bad_transform = tmp_path / "bad_transform.py"
    bad_transform.write_text(
        textwrap.dedent("""\
        def transform(instances, extra_param):
            return instances
        """),
        encoding="utf-8",
    )

    class TransformArityModel(Incorporator):
        pass

    result = await TransformArityModel.incorp(inc_file=str(json_file))
    with pytest.raises(ValueError, match="exactly 1 parameter"):
        await TransformArityModel.export(
            instance=result,
            file_path=str(tmp_path / "out.json"),
            code_file=str(bad_transform),
        )


@pytest.mark.asyncio
async def test_code_file_transform_correct_arity_passes(tmp_path: Path) -> None:
    """_apply_code_transform must succeed when transform has exactly 1 parameter."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    good_transform = tmp_path / "good_transform.py"
    good_transform.write_text(
        textwrap.dedent("""\
        def transform(instances):
            return [{"id": obj.id, "name": obj.name, "upper_name": obj.name.upper()} for obj in instances]
        """),
        encoding="utf-8",
    )

    class TransformGoodModel(Incorporator):
        pass

    result = await TransformGoodModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_path = tmp_path / "out.json"
    await TransformGoodModel.export(instance=result, file_path=str(out_path), code_file=str(good_transform))

    content = out_path.read_text(encoding="utf-8")
    assert "ALICE" in content


# ==========================================
# 3. code_file SCHEMA DRIFT
# ==========================================


@pytest.mark.asyncio
async def test_export_code_file_new_field_in_csv(tmp_path: Path) -> None:
    """When code_file adds a new field, it must appear as a column in CSV output."""
    json_file = tmp_path / "data.json"
    json_file.write_text(json.dumps([{"id": 1, "name": "Alice"}]), encoding="utf-8")

    transform_file = tmp_path / "transform.py"
    transform_file.write_text(
        textwrap.dedent("""\
        def transform(instances):
            return [
                {"id": obj.id, "name": obj.name, "synthesized_field": "generated"}
                for obj in instances
            ]
        """),
        encoding="utf-8",
    )

    class DriftTestModel(Incorporator):
        pass

    result = await DriftTestModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")
    out_csv = tmp_path / "out.csv"
    await DriftTestModel.export(
        instance=result,
        file_path=str(out_csv),
        code_file=str(transform_file),
    )

    content = out_csv.read_text(encoding="utf-8")
    # The synthesized_field column must appear in the CSV header
    assert "synthesized_field" in content
    assert "generated" in content


# ==========================================
# 4. _schema_union CONCURRENT SAFETY
# ==========================================


@pytest.mark.asyncio
async def test_schema_union_concurrent_gather_safety(tmp_path: Path) -> None:
    """Two concurrent incorp() calls on the same class must produce a complete schema union."""
    json1 = tmp_path / "f1.json"
    json2 = tmp_path / "f2.json"
    # 2+ items required: single-item files trigger the is_single path which skips _schema_union
    json1.write_text(json.dumps([{"unique_field_alpha": 1}, {"unique_field_alpha": 2}]), encoding="utf-8")
    json2.write_text(json.dumps([{"unique_field_beta": 3}, {"unique_field_beta": 4}]), encoding="utf-8")

    class ConcurrentModel(Incorporator):
        pass

    # Force reset in case prior test left state
    if "_schema_union" in ConcurrentModel.__dict__:
        del ConcurrentModel._schema_union  # type: ignore[attr-defined]

    await asyncio.gather(
        ConcurrentModel.incorp(inc_file=str(json1)),
        ConcurrentModel.incorp(inc_file=str(json2)),
    )

    # Both fields must appear in the union regardless of which call finished first
    assert "unique_field_alpha" in ConcurrentModel._schema_union  # type: ignore[attr-defined]
    assert "unique_field_beta" in ConcurrentModel._schema_union  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_schema_union_sibling_class_isolation(tmp_path: Path) -> None:
    """Sibling subclasses must not share _schema_union state."""
    json_a = tmp_path / "a.json"
    json_b = tmp_path / "b.json"
    # 2+ items required: single-item files trigger the is_single path which skips _schema_union
    json_a.write_text(json.dumps([{"field_only_in_a": 1}, {"field_only_in_a": 2}]), encoding="utf-8")
    json_b.write_text(json.dumps([{"field_only_in_b": 3}, {"field_only_in_b": 4}]), encoding="utf-8")

    class SiblingA(Incorporator):
        pass

    class SiblingB(Incorporator):
        pass

    # Clear any residual state
    for cls in (SiblingA, SiblingB):
        if "_schema_union" in cls.__dict__:
            del cls._schema_union  # type: ignore[attr-defined]

    await SiblingA.incorp(inc_file=str(json_a))
    await SiblingB.incorp(inc_file=str(json_b))

    a_union = SiblingA._schema_union  # type: ignore[attr-defined]
    b_union = SiblingB._schema_union  # type: ignore[attr-defined]

    assert "field_only_in_a" in a_union
    assert "field_only_in_b" not in a_union  # Sibling B's field must NOT bleed into A

    assert "field_only_in_b" in b_union
    assert "field_only_in_a" not in b_union  # Sibling A's field must NOT bleed into B


# ==========================================
# 5. DYNAMIC MODEL IN-STATE EXPORT
# ==========================================


@pytest.mark.asyncio
async def test_dynamic_model_in_state_export(tmp_path: Path) -> None:
    """In-state export (no file_path arg) must write all records from cls.inc_dict."""
    json_file = tmp_path / "data.json"
    json_file.write_text(
        json.dumps([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]),
        encoding="utf-8",
    )

    class InStateModel(Incorporator):
        pass

    await InStateModel.incorp(inc_file=str(json_file), inc_code="id", inc_name="name")

    out_path = tmp_path / "out.json"
    # In-state mode: instance is the output path, data comes from cls.inc_dict
    await InStateModel.export(instance=str(out_path))

    content = out_path.read_text(encoding="utf-8")
    assert "Alice" in content
    assert "Bob" in content
