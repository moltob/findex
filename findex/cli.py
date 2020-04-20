import logging
import os
import pathlib

from findex.files import Index

INDEX_DIR = pathlib.Path(r'.')
INDEX_DB = pathlib.Path('test.db')


def main():
    import colorama
    colorama.init()

    import daiquiri
    daiquiri.setup(level=logging.INFO)

    if 'INDEX_DEVELOPER' in os.environ:
        INDEX_DB.unlink(missing_ok=True)

    findex = Index(pathlib.Path(INDEX_DB))
    findex.create()
    findex.add_directory(INDEX_DIR)


if __name__ == '__main__':
    main()
