"""Index of files in a directory structure."""
import collections
import contextlib
import logging
import pathlib
import sqlite3

import click
import tqdm

from findex.db import Storage, DbClosedError
from findex.fs import FileDesc, FILEHASH_WALK_ERROR, count_files, walk

_logger = logging.getLogger(__name__)

FileFrom = collections.namedtuple("FileFrom", "fhash origin path size created modified")
"""Descriptor for a file with origin as used in comparison."""


class Index(Storage):
    """Index of file path by content, based on sqlite."""

    def create(self, path: pathlib.Path):
        """Create index of given directory."""

        _logger.info(f"Creating index of {path}.")
        self.create_db()
        errors = []

        def _on_error(error: OSError):
            _logger.warning(error)
            errors.append(
                FileDesc(
                    path=error.filename,
                    size=0,
                    fhash=FILEHASH_WALK_ERROR.format(message=error.strerror),
                    created=None,
                    modified=None,
                )
            )

        click.echo(f"Counting files in {path}...")
        count = count_files(path, _on_error)

        with contextlib.closing(self.open()):
            _logger.debug(f"Writing {len(errors)} walk errors to database.")
            for errordesc in errors:
                self._add_file(errordesc)

            _logger.info(f"Found {count} files to be added to index.")
            for filedesc in tqdm.tqdm(
                walk(path), total=count, desc="Read", unit="files"
            ):
                self._add_file(filedesc)
                self._on_update()

    def _add_file(self, filedesc: FileDesc):
        try:
            self.connection.execute(
                "INSERT INTO file (path,size,hash,created,modified)"
                "  VALUES (?,?,?,datetime(?),datetime(?));",
                filedesc,
            )
        except sqlite3.OperationalError:
            _logger.error(f"Cannot add file to database: {filedesc}")
            raise

    def __iter__(self):
        if not self.opened:
            _logger.error("Database closed, cannot iterate.")
            raise DbClosedError()

        with contextlib.closing(self.connection.cursor()) as cursor:
            for row in cursor.execute(
                "SELECT path,size,hash,created,modified from file"
            ):
                yield FileDesc._make(row)

    @property
    def duplicates(self):
        """Return list of duplicate files."""
        raise NotImplementedError()


class Comparison(Storage):
    """Comparison of two index databases."""

    def create(self, index1_path: pathlib.Path, index2_path: pathlib.Path):
        """Create comparison of two file index files."""

        _logger.info(f"Creating comparison of {index1_path} and {index2_path}.")

        self.create_db()

        with contextlib.closing(self.open()):
            click.echo(f"\nAdding data from {index1_path} (index 1).")
            self._add_index(index1_path, 1)
            click.echo(f"\nAdding data from {index2_path} (index 2).")
            self._add_index(index2_path, 2)

    def _add_index(self, index_path: pathlib.Path, index_id: int):
        index = Index(index_path).open()

        for file in tqdm.tqdm(index, unit="files"):
            self._add_file(file, index_id)
            self._on_update()

    def _add_file(self, filedesc: FileDesc, index_id: int):

        # extend fie descriptor with origin:
        data = filedesc._asdict()
        data["origin"] = index_id
        filefrom = FileFrom(**data)

        try:
            self.connection.execute(
                "INSERT INTO file (hash,origin,path,size,created,modified)"
                "  VALUES (?,?,?,?,datetime(?),datetime(?));",
                filefrom,
            )
        except sqlite3.OperationalError:
            _logger.error(f"Cannot add file to database: {filedesc}")
            raise

    @property
    def missing_files(self):
        """Return files only in index 1, but not in 2."""
        raise NotImplementedError()

    @property
    def new_files(self):
        """Return files only in index 2, but not in 1."""
        raise NotImplementedError()

    @property
    def files_map(self):
        """Return list of pairs of files in index 1 and their corresponding files in index 2.

        In case of duplicates in an index, there is possibly more than one file in each of the
        elements.
        """
        raise NotImplementedError()
