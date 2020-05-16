"""Index utilities."""
import contextlib
import datetime
import logging
import pathlib
import sqlite3
import typing as t

import findex

DATABASE_TRANSACTION_SIZE = 10000
"""Maximum number of data sets before writing to database."""

META_DATE = "DATE"
META_VERSION = "VERSION"

_logger = logging.getLogger(__name__)


class DbExistsError(Exception):
    """The index exists and cannot be recreated."""


class DbClosedError(Exception):
    """The index is closed and cannot be processed."""


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

        parent = self.path.parent
        if not parent.exists():
            parent.mkdir(parents=True)

        # compute path to schema of derived class:
        schema_folder = pathlib.Path(__file__).parent / "schema"
        schema_paths = (
            schema_folder / f"{name}.sql" for name in (self.__class__.__name__, "Meta")
        )

        _logger.info(f"Creating database {self.path}.")
        with contextlib.closing(self.open()):
            for schema_path in schema_paths:
                self.connection.executescript(schema_path.read_text())

            self._put_meta(META_DATE, str(datetime.datetime.now()))
            self._put_meta(META_VERSION, findex.__version__)

    def open(self):
        if self.opened:
            _logger.warning("Database already open.")
            return self

        _logger.info(f"Opening database {self.path}.")
        self.connection = sqlite3.connect(
            self.path, detect_types=sqlite3.PARSE_DECLTYPES
        )
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

    def _put_meta(self, key: str, value: str):
        """Add meta information to storage."""
        assert self.connection, "database must be open"
        self.connection.execute(
            "INSERT INTO meta (key,value) VALUES (?,?);", (key, value)
        )

    def _get_meta(self, key: str) -> t.Optional[str]:
        """Returns value for given key or None if not found."""
        assert self.connection, "database must be open"
        return self.connection.execute(
            "SELECT value FROM meta WHERE key=(?)", (key,)
        ).fetchone()[0]


@contextlib.contextmanager
def opened_storage(storage: Storage):
    opened_before = storage.opened

    try:
        if not opened_before:
            storage.open()
        yield storage
    finally:
        if not opened_before:
            storage.close()
