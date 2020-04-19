"""Index of files in a directory structure."""
import collections
import logging
import pathlib
import sqlite3

FILEHASH_EMPTY = '0000000000000000000000000000000000000000'
"""Fake hash for empty files."""

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
            _logger.error('Database already open.')
            return

        self.connection = sqlite3.connect(self.path)

    def close(self):
        if self.opened:
            self.connection.close()
            self.connection = None
