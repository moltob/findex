"""Index of files in a directory structure."""
import collections
import hashlib
import logging
import mmap
import os
import pathlib
import sqlite3
import typing as t

# fake hash values to identify non-hashable files:
FILEHASH_EMPTY = 'empty'.ljust(40, '.')
FILEHASH_INACCESSIBLE = 'inaccessible'.ljust(40, '.')

FileDesc = collections.namedtuple('FileDesc', 'path size fhash')
"""Descriptor for a file in index."""

SCHEMA_FILE = 'findex-schema.sql'

_logger = logging.getLogger(__name__)


class Index:
    """Index of file path by content, based on sqlite."""

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.connection = None

    @property
    def exists(self):
        return self.path and self.path.exists()

    @property
    def opened(self):
        return bool(self.connection)

    def create(self):
        """Create and open sqlite DB with index schema."""
        if self.exists:
            _logger.error(f'Database file at {self.path} already exists, delete first.')
            return

        _logger.info(f'Creating file index database {self.path}.')
        self.open()
        self.connection.executescript(pathlib.Path(SCHEMA_FILE).read_text())

    def open(self):
        if self.opened:
            _logger.warning('Database already open.')
            return

        _logger.debug(f'Opening database {self.path}.')
        self.connection = sqlite3.connect(self.path)

    def close(self):
        if self.opened:
            self.connection.close()
            self.connection = None
            _logger.debug('Database closed.')

    def add_directory(self, path: pathlib.Path):
        """Adds the files in directory to index."""
        if not self.opened:
            _logger.error('Index is not opened, cannot add data.')
            return

        _logger.info(f'Adding {path} to index.')


def count_files(top: pathlib.Path) -> int:
    count = 0
    for dirpath, dirnames, filenames in os.walk(top):
        count += len(filenames)
    return count


def count_bytes(top: pathlib.Path) -> int:
    count = 0
    for dirpath, dirnames, filenames in os.walk(top):
        for filename in filenames:
            count += (top / dirpath / filename).stat().st_size
    return count


def index_directory(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path, int]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    _logger.info(f'Traversing directory {top} recursively.')

    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath).absolute()
        for filename in filenames:
            filepath = root / filename
            filesize = filepath.stat().st_size

            if filesize == 0:
                filehash = FILEHASH_EMPTY
            else:
                filehash = compute_filehash(filepath)

            _logger.debug(f'{filehash} {filepath}')
            yield FileDesc(path=filepath, fhash=filehash, size=filesize)


def compute_filehash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()
