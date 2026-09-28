"""Tests for simulator ground truth isolation and schema compliance."""
from __future__ import annotations

import os
from pathlib import Path
import pytest


def test_app_does_not_reference_ground_truth():
    """Verify that the API and every module under app/ NEVER references ground_truth."""
    app_dir = Path("app")
    assert app_dir.is_dir(), "app directory not found"

    violations = []
    for py_file in app_dir.rglob("*.py"):
        text = py_file.read_text(encoding="utf-8")
        lines = text.splitlines()
        for idx, line in enumerate(lines, 1):
            if "ground_truth" in line.lower():
                violations.append(f"{py_file}:{idx}: {line.strip()}")

    assert not violations, (
        f"Found {len(violations)} prohibited reference(s) to 'ground_truth' in app/:\n"
        + "\n".join(violations)
    )
