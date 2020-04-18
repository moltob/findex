"""Index of files in a directory structure.

Map of content SHA1 hash to file path.
"""
import hashlib
import logging
import mmap
import os
import pathlib
import sqlite3

import typing as t

_logger = logging.getLogger(__name__)

FILEHASH_EMPTY = '0000000000000000000000000000000000000000'
"""Fake hash for empty files."""

SCHEMA_FILE = 'findex-schema.sql'


def index_directory(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path, int]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath).absolute()
        for filename in filenames:
            filepath = root / filename
            filesize = filepath.stat().st_size

            if filesize == 0:
                filehash = FILEHASH_EMPTY
            else:
                filehash = compute_hash(filepath)
            yield filepath, filehash, filesize


def compute_hash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()


class Index:
    """Index of file path by content, based on sqlite."""

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.connection = None

    @property
    def connected(self):
        return bool(self.connection)

    def create(self):
        """Create and open sqlite DB with index schema."""
        if self.connected:
            _logger.error('Cannot create database if already connected.')
            return

        if self.path.exists():
            _logger.error(f'Database file at {self.path} already exists, delete first.')

        _logger.info(f'Creating file index database {self.path}.')
        self.connection = sqlite3.connect(self.path)
        self.connection.executescript(pathlib.Path(SCHEMA_FILE).read_text())

    def close(self):
        if self.connected:
            self.connection.close()
            self.connection = None


if __name__ == '__main__':
    result = None

    findex = Index(pathlib.Path('test.db'))
    findex.create()

    # def timed_main():
    #     global result
    #     result = list(index_directory(pathlib.Path(r'Q:\Britta Pagel Fotografie')))
    #
    #
    # import timeit
    #
    # exec_time = timeit.timeit(timed_main, number=1)
    #
    # numbytes = 0
    # for fpath, fhash, fsize in result:
    #     print(fhash, fpath)
    #     numbytes += fsize
    #
    # kbits = 8 / 1024 * numbytes / exec_time
    # print(f'Execution time: {exec_time:.3f} s')
    # print(f'Files:          {len(result)} ({numbytes:,d} bytes)')
    # print(f'Bandwith:       {kbits:.2f} kbit/s')
