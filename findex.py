"""Index of files in a directory structure."""
import logging
import pathlib
import sqlite3

from fhash import count_files, index_directory

_logger = logging.getLogger(__name__)

SCHEMA_FILE = 'findex-schema.sql'


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
