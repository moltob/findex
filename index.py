"""Index of files in a directory structure.

Map of content SHA1 hash to file path.
"""
import hashlib
import logging
import mmap
import os
import pathlib

import typing as t

_logger = logging.getLogger(__name__)


def index_directory(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath).absolute()
        for filename in filenames:
            filepath = root / filename

            if filepath.stat().st_size == 0:
                _logger.warning(f'Empty file not indexed: {filepath}')
                continue

            filehash = compute_hash(filepath)
            yield filepath, filehash


def compute_hash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()


if __name__ == '__main__':
    for fpath, fhash in index_directory(pathlib.Path()):
        print(fpath, fhash)
        pass
