import pathlib

import pytest

from findex.index import Index, Comparison


@pytest.fixture(scope="module")
def comparison(cwd_module_dir, output_dir):
    input_dir = pathlib.Path("input")

    index1 = Index(output_dir / "index1.db")
    index1.create(input_dir / "folder1")

    index2 = Index(output_dir / "index2.db")
    index2.create(input_dir / "folder2")

    comparison = Comparison(output_dir / "comparison.db")
    comparison.create(index1, index2)
    return comparison


def test__missing_files(comparison):
    files = list(pathlib.Path(f.path) for f in comparison.iter_missing())
    assert len(files) == 1
    assert pathlib.Path("missing1.txt") in files

    # include updated files:
    files = list(
        pathlib.Path(f.path) for f in comparison.iter_missing(include_updated=True)
    )
    assert len(files) == 2
    assert pathlib.Path("missing1.txt") in files
    assert pathlib.Path("updated1.txt") in files


def test__new_files(comparison):
    files = list(pathlib.Path(f.path) for f in comparison.iter_new())
    assert len(files) == 1
    assert pathlib.Path("sub2", "new1.txt") in files

    # include updated files:
    files = list(
        pathlib.Path(f.path) for f in comparison.iter_new(include_updated=True)
    )
    assert len(files) == 2
    assert pathlib.Path("sub2", "new1.txt") in files
    assert pathlib.Path("updated1.txt") in files


def test__updated_files(comparison):
    files = list(pathlib.Path(f.path) for f in comparison.iter_updated())
    assert len(files) == 1
    assert pathlib.Path("updated1.txt") in files


def test__content_groups(comparison):
    file_groups = [
        (set(map(pathlib.Path, g.files1)), set(map(pathlib.Path, g.files2)))
        for g in comparison.iter_content_groups()
    ]

    assert len(file_groups) == 4

    assert (
        {pathlib.Path("single.txt")},
        {pathlib.Path("sub2", "single.txt")},
    ) in file_groups

    assert (
        {pathlib.Path("same1.txt"), pathlib.Path("sub1", "same1_duplicate1.txt")},
        {pathlib.Path("same1.txt")},
    ) in file_groups

    assert (
        {pathlib.Path("sub1", "same2.txt")},
        {pathlib.Path("same2-moved.txt")},
    ) in file_groups

    assert (
        {pathlib.Path("empty1.txt"), pathlib.Path("sub1", "empty2.txt")},
        {pathlib.Path("empty3.txt")},
    ) in file_groups
