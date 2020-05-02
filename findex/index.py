"""Index of files in a directory structure."""
import contextlib
import logging
import pathlib
import sqlite3

import click
import tqdm

from findex.db import Storage
from findex.fs import FileDesc, FILEHASH_WALK_ERROR, count_files, walk

_logger = logging.getLogger(__name__)


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

    def __len__(self):
        self._ensure_opened()
        return self.connection.execute("SELECT COUNT(*) from file").fetchone()[0]

    def __iter__(self):
        self._ensure_opened()

        with contextlib.closing(self.connection.cursor()) as cursor:
            for row in cursor.execute(
                "SELECT path,size,hash,created,modified from file"
            ):
                yield FileDesc._make(row)

    @property
    def iter_duplicates(self):
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
            self._add_index(index1_path, "file1")
            click.echo(f"\nAdding data from {index2_path} (index 2).")
            self._add_index(index2_path, "file2")

    def _add_index(self, index_path: pathlib.Path, table: str):
        index = Index(index_path).open()

        for file in tqdm.tqdm(index, unit="files"):
            self._add_file(file, table)
            self._on_update()

    def _add_file(self, filedesc: FileDesc, table: str):
        try:
            self.connection.execute(
                f"INSERT INTO {table} (path,size,hash,created,modified)"
                f"  VALUES (?,?,?,datetime(?),datetime(?));",
                filedesc,
            )
        except sqlite3.OperationalError:
            _logger.error(f"Cannot add file to database: {filedesc}")
            raise

    def _iter_exclusive_files(self, table_contained, table_not_contained):
        """Return files only in table_contained, but not in table_not_contained."""
        assert table_contained != table_not_contained
        
        self._ensure_opened()
        with contextlib.closing(self.connection.cursor()) as cursor:
            for row in cursor.execute(
                f"SELECT "
                f"  {table_contained}.path,"
                f"  {table_contained}.size,"
                f"  {table_contained}.hash,"
                f"  {table_contained}.created,"
                f"  {table_contained}.modified "
                f"FROM {table_contained} LEFT OUTER JOIN {table_not_contained} "
                f"  ON {table_contained}.hash = {table_not_contained}.hash "
                f"WHERE {table_not_contained}.hash IS NULL"
            ):
                yield FileDesc._make(row)

    def iter_missing_files(self):
        """Return files only in index 1, but not in 2."""
        return self._iter_exclusive_files('file1', 'file2')

    def iter_new_files(self):
        """Return files only in index 2, but not in 1."""
        return self._iter_exclusive_files('file2', 'file1')

    def iter_files_map(self):
        """Return list of pairs of files in index 1 and their corresponding files in index 2.

        In case of duplicates in an index, there is possibly more than one file in each of the
        elements.
        """
        raise NotImplementedError()
