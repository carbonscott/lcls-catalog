"""Pytest fixtures for LCLS catalog tests."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import pytest


@dataclass
class FakeFile:
    """Represents a file in the fake directory structure."""

    path: Path
    size: int


@dataclass
class ExpStructure:
    """Represents a fake LCLS experiment directory structure."""

    root: Path
    experiment: str
    experiment_path: Path
    files: Dict[str, FakeFile]
    expected_file_count: int
    expected_total_size: int


def create_file(path: Path, size: int) -> FakeFile:
    """Create a file with specified size."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"x" * size)
    return FakeFile(path=path, size=size)


@pytest.fixture
def fake_experiment(tmp_path) -> ExpStructure:
    """
    Create a fake LCLS experiment directory structure.

    Structure:
    tmp_path/
    └── exp/
        └── xpp/
            └── xpptest01/
                ├── scratch/
                │   ├── run0001/
                │   │   ├── image_0001.h5  (1024 bytes)
                │   │   ├── image_0002.h5  (2048 bytes)
                │   │   └── metadata.json  (100 bytes)
                │   ├── run0002/
                │   │   └── data.h5        (512 bytes)
                │   └── empty_run/         (empty directory)
                ├── results/
                │   └── analysis.npz       (256 bytes)
                └── calib/
                    └── calibration.dat    (128 bytes)
    """
    experiment = "xpptest01"
    exp_path = tmp_path / "exp" / "xpp" / experiment

    files = {}

    # scratch/run0001
    files["image_0001.h5"] = create_file(
        exp_path / "scratch" / "run0001" / "image_0001.h5", 1024
    )
    files["image_0002.h5"] = create_file(
        exp_path / "scratch" / "run0001" / "image_0002.h5", 2048
    )
    files["metadata.json"] = create_file(
        exp_path / "scratch" / "run0001" / "metadata.json", 100
    )

    # scratch/run0002
    files["data.h5"] = create_file(
        exp_path / "scratch" / "run0002" / "data.h5", 512
    )

    # scratch/empty_run (empty directory)
    (exp_path / "scratch" / "empty_run").mkdir(parents=True, exist_ok=True)

    # results
    files["analysis.npz"] = create_file(
        exp_path / "results" / "analysis.npz", 256
    )

    # calib
    files["calibration.dat"] = create_file(
        exp_path / "calib" / "calibration.dat", 128
    )

    expected_total_size = sum(f.size for f in files.values())

    return ExpStructure(
        root=tmp_path,
        experiment=experiment,
        experiment_path=exp_path,
        files=files,
        expected_file_count=len(files),
        expected_total_size=expected_total_size,
    )


@pytest.fixture
def deep_nested_structure(tmp_path) -> ExpStructure:
    """Create a deeply nested directory structure (10+ levels)."""
    experiment = "xppdeep01"
    exp_path = tmp_path / "exp" / "xpp" / experiment

    files = {}

    # Create 12 levels of nesting
    current_path = exp_path
    for i in range(12):
        current_path = current_path / f"level{i:02d}"

    files["deep_file.txt"] = create_file(current_path / "deep_file.txt", 64)

    return ExpStructure(
        root=tmp_path,
        experiment=experiment,
        experiment_path=exp_path,
        files=files,
        expected_file_count=1,
        expected_total_size=64,
    )


@pytest.fixture
def special_chars_structure(tmp_path) -> ExpStructure:
    """Create structure with special characters in filenames."""
    experiment = "xppspecial01"
    exp_path = tmp_path / "exp" / "xpp" / experiment

    files = {}

    # Files with spaces
    files["file with spaces.txt"] = create_file(
        exp_path / "dir with spaces" / "file with spaces.txt", 32
    )

    # Files with unicode
    files["data_2024.h5"] = create_file(
        exp_path / "unicode" / "data_2024.h5", 64
    )

    # Files with multiple dots
    files["archive.tar.gz.bak"] = create_file(
        exp_path / "dots" / "archive.tar.gz.bak", 48
    )

    # Hidden files
    files[".hidden_config"] = create_file(
        exp_path / "hidden" / ".hidden_config", 16
    )

    expected_total_size = sum(f.size for f in files.values())

    return ExpStructure(
        root=tmp_path,
        experiment=experiment,
        experiment_path=exp_path,
        files=files,
        expected_file_count=len(files),
        expected_total_size=expected_total_size,
    )


@pytest.fixture
def symlink_structure(tmp_path) -> ExpStructure:
    """Create structure with symbolic links."""
    experiment = "xppsymlink01"
    exp_path = tmp_path / "exp" / "xpp" / experiment

    files = {}

    # Regular file
    files["original.dat"] = create_file(
        exp_path / "data" / "original.dat", 128
    )

    # Create symlink to file
    symlink_path = exp_path / "links" / "link_to_original.dat"
    symlink_path.parent.mkdir(parents=True, exist_ok=True)
    symlink_path.symlink_to(files["original.dat"].path)

    # Create broken symlink
    broken_link = exp_path / "links" / "broken_link.dat"
    broken_link.symlink_to("/nonexistent/path/file.dat")

    return ExpStructure(
        root=tmp_path,
        experiment=experiment,
        experiment_path=exp_path,
        files=files,
        expected_file_count=1,
        expected_total_size=128,
    )


@pytest.fixture
def catalog_db(tmp_path) -> Path:
    """Return a path for a test catalog database."""
    return tmp_path / "test_catalog.db"
