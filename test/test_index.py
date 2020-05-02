import pathlib

import pytest

from findex.index import Index, Comparison


@pytest.fixture(scope='module')
def comparison(cwd_module_dir, output_dir):
    input_dir = pathlib.Path('input')

    index1 = Index(output_dir / 'index1.db')
    index1.create(input_dir / 'folder1')

    index2 = Index(output_dir / 'index2.db')
    index2.create(input_dir / 'folder2')

    comparison = Comparison(output_dir / 'comparison.db')
    comparison.create(index1.path, index2.path)
    return comparison


def test__new_files(comparison):
    new_files = list(pathlib.Path(f.path) for f in comparison.iter_new_files())
    assert pathlib.Path('sub2', 'new1.txt') in new_files

    # for now also changed files are considered new:
    assert len(new_files) == 2
    assert pathlib.Path('updated1.txt') in new_files
