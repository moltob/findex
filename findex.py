"""Index of files in a directory structure."""
import collections
import contextlib
import hashlib
import logging
import mmap
import os
import pathlib
import sqlite3
import typing as t

# fake hash values to identify non-hashable files:
import tqdm

FILEHASH_EMPTY = 'empty'.ljust(40, '.')
FILEHASH_INACCESSIBLE = 'inaccessible'.ljust(40, '.')

FileDesc = collections.namedtuple('FileDesc', 'path size fhash')
"""Descriptor for a file in index."""

SCHEMA_FILE = 'findex-schema.sql'

DATABASE_TRANSACTION_SIZE = 100000
"""Maximum number of data sets before writing to database."""

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
        self.close()

    def open(self):
        if self.opened:
            _logger.warning('Database already open.')
            return

        _logger.info(f'Opening database {self.path}.')
        self.connection = sqlite3.connect(self.path)

        return self

    def close(self):
        if self.opened:
            self._flush()
            self.connection.close()
            self.connection = None
            _logger.info('Database closed.')

    def add_directory(self, path: pathlib.Path):
        """Adds the files in directory to index."""
        _logger.info(f'Adding {path} to index.')
        count = count_files(path)

        _logger.info(f'Found {count} files to be added to index.')

        with contextlib.closing(self.open()):
            files_before_flush = DATABASE_TRANSACTION_SIZE
            for filedesc in tqdm.tqdm(walk(path), total=count, desc='Read', unit='files'):
                self._add_file(filedesc)
                files_before_flush -= 1

                if files_before_flush <= 0:
                    self._flush()
                    files_before_flush = DATABASE_TRANSACTION_SIZE

    def _add_file(self, filedesc: FileDesc):
        try:
            self.connection.execute(
                f'INSERT INTO file (path,size,hash) VALUES (?,?,?);',
                filedesc,
            )
        except sqlite3.OperationalError as ex:
            _logger.error(f'Cannot add file to database: {filedesc}')
            raise

    def _flush(self):
        _logger.debug('Flushing transaction.')
        self.connection.commit()


def count_files(top: pathlib.Path) -> int:
    _logger.debug(f'Counting files in {top}.')

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


def walk(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path, int]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    _logger.debug(f'Traversing directory {top} recursively.')

    for dirpath, dirnames, filenames in os.walk(top):
        root = (top / dirpath).absolute()
        for filename in filenames:
            filepath = root / filename
            filesize = filepath.stat().st_size

            if filesize == 0:
                filehash = FILEHASH_EMPTY
            else:
                try:
                    filehash = compute_filehash(filepath)
                except PermissionError:
                    _logger.warning(f'File inaccessible: {filepath}.')
                    filehash = FILEHASH_INACCESSIBLE

            _logger.debug(f'{filehash} {filepath}')
            yield FileDesc(path=str(filepath), fhash=filehash, size=filesize)


def compute_filehash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()
