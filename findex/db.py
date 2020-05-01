"""Index utilities."""
import logging
import pathlib
import sqlite3

DATABASE_TRANSACTION_SIZE = 10000
"""Maximum number of data sets before writing to database."""

_logger = logging.getLogger(__name__)


class DbExistsError(Exception):
    """The index exists and cannot be recreated."""


class Storage:
    """Base class for sqlite-based storage."""

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.connection = None
        self.updates_before_flush = DATABASE_TRANSACTION_SIZE

    @property
    def exists(self):
        return self.path and self.path.exists()

    @property
    def opened(self):
        return bool(self.connection)

    def create_db(self):
        """Create and open sqlite DB with index schema."""
        if self.exists:
            _logger.error(f"Database already exists at {self.path}.")
            raise DbExistsError(self.path)

        # compute path to schema of derived class:
        schema_path = (
            pathlib.Path(__file__).parent / "schema" / f"{self.__class__.__name__}.sql"
        )

        _logger.info(f"Creating database {self.path}.")
        self.open()
        self.connection.executescript(schema_path.read_text())
        self.close()

    def open(self):
        if self.opened:
            _logger.warning("Database already open.")
            return self

        _logger.info(f"Opening database {self.path}.")
        self.connection = sqlite3.connect(self.path)
        self.updates_before_flush = DATABASE_TRANSACTION_SIZE

        return self

    def close(self):
        if self.opened:
            self._flush()
            self.connection.close()
            self.connection = None
            _logger.info("Database closed.")

    def _on_update(self):
        """Called to inform about written access to database, leading to periodic flushing."""
        if self.updates_before_flush > 0:
            self.updates_before_flush -= 1
        else:
            self._flush()
            self.updates_before_flush = DATABASE_TRANSACTION_SIZE

    def _flush(self):
        _logger.debug("Flushing transaction.")
        self.connection.commit()
