import argparse
import logging
import os
import pathlib

from findex.files import Index


def main():
    parser = argparse.ArgumentParser(description='Hash-based directory indexer')
    parser.add_argument(
        'directory',
        help='Path to directory for which index is created.',
    )
    parser.add_argument(
        '--index',
        help='Path to created index file.',
        default='findex.db'
    )
    parser.add_argument(
        '--overwrite',
        help='Flag whether to overwrite an existing index file. Previous data will be lost.',
        action='store_true',
    )

    import colorama
    colorama.init()

    import daiquiri
    daiquiri.setup(level=logging.INFO)

    args = parser.parse_args()

    index_path = pathlib.Path(args.index)
    directory_path = pathlib.Path(args.directory)

    if args.overwrite:
        index_path.unlink(missing_ok=True)

    findex = Index(index_path)
    findex.create()
    findex.add_directory(directory_path)


if __name__ == '__main__':
    main()
