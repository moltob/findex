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

FILEHASH_EMPTY = '0000000000000000000000000000000000000000'
"""Fake hash for empty files."""


def index_directory(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath).absolute()
        for filename in filenames:
            filepath = root / filename

            if filepath.stat().st_size == 0:
                filehash = FILEHASH_EMPTY
            else:
                filehash = compute_hash(filepath)
            yield filepath, filehash


def compute_hash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()


if __name__ == '__main__':
    result = None

    def timed_main():
        global result
        result = list(index_directory(pathlib.Path()))

    import timeit
    exec_time = timeit.timeit(timed_main, number=5)

    for fpath, fhash in result:
        print(fhash, fpath)

    print(f'Execution time: {exec_time:.3f} s.')
