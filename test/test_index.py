import pathlib

from findex.index import Index, Comparison


def test__new_files(output_dir):
    dir_in = pathlib.Path('input')
    dir_out = output_dir / 'new'

    index1 = Index(dir_out / 'index1.db')
    index1.create(dir_out / 'folder1')

    index2 = Index(dir_out / 'index2.db')
    index2.create(dir_out / 'folder2')

    comparison = Comparison(dir_out / 'comparison.db')
    comparison.create(index1.path, index2.path)

