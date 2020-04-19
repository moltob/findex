import logging
import pathlib

from findex import walk, count_files, count_bytes, Index

TEST_DIR = pathlib.Path(r'.')
TEST_DB = pathlib.Path('test.db')


def main():
    import colorama
    colorama.init()

    import daiquiri
    daiquiri.setup(level=logging.INFO)

    TEST_DB.unlink(missing_ok=True)

    findex = Index(pathlib.Path(TEST_DB))
    findex.create()
    findex.add_directory(TEST_DIR)


if __name__ == '__main__':
    main()
