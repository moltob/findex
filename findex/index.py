"""Index of files in a directory structure."""
import collections
import contextlib
import logging
import pathlib
import sqlite3

import click
import tqdm

from findex.db import Storage, opened_storage
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

        with opened_storage(self):
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

    def count(self):
        with opened_storage(self):
            return self.connection.execute("SELECT COUNT(*) from file").fetchone()[0]

    def iter_all(self):
        with opened_storage(self):
            with contextlib.closing(self.connection.cursor()) as cursor:
                for row in cursor.execute(
                    "SELECT path,size,hash,created,modified from file"
                ):
                    yield FileDesc._make(row)

    @property
    def iter_duplicates(self):
        """Return list of duplicate files."""
        raise NotImplementedError()


FilesMap = collections.namedtuple("FilesMap", "fhash files1 files2")
"""Files in comparison with identical content hash."""


class Comparison(Storage):
    """Comparison of two index databases."""

    def create(self, index1: Index, index2: Index):
        """Create comparison of two file index files."""

        _logger.info(f"Creating comparison {self.path}.")

        self.create_db()

        with opened_storage(self):
            click.echo(f"\nAdding data from {index1.path}.")
            self._add_index(index1, "file1")
            click.echo(f"\nAdding data from {index2.path}.")
            self._add_index(index2, "file2")

    def _add_index(self, index: Index, table: str):
        for file in tqdm.tqdm(index.iter_all(), total=index.count(), unit="files"):
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

    def _iter_exclusive_files(
        self, table_contained, table_not_contained, *, include_updated=False
    ):
        """Return files only in table_contained, but not in table_not_contained."""
        assert table_contained != table_not_contained

        if include_updated:
            excluded_paths = {}
        else:
            excluded_paths = {f.path for f in self.iter_updated()}

        with opened_storage(self):
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
                    file = FileDesc._make(row)

                    if file.path not in excluded_paths:
                        yield file

    def iter_missing(self, *, include_updated=False):
        """Return files only in index 1, but not in 2."""
        return self._iter_exclusive_files(
            "file1", "file2", include_updated=include_updated
        )

    def iter_new(self, *, include_updated=False):
        """Return files only in index 2, but not in 1."""
        return self._iter_exclusive_files(
            "file2", "file1", include_updated=include_updated
        )

    def iter_updated(self):
        """Return files that have an identical path but different hashes."""
        with opened_storage(self):
            with contextlib.closing(self.connection.cursor()) as cursor:
                for row in cursor.execute(
                    "SELECT "
                    "  file1.path,"
                    "  file1.size,"
                    "  file1.hash,"
                    "  file1.created,"
                    "  file1.modified "
                    "FROM file1 JOIN file2 "
                    "  ON file1.path = file2.path "
                    "WHERE file1.hash != file2.hash"
                ):
                    yield FileDesc._make(row)

    def iter_content_groups(self):
        """Return list of pairs of paths in index 1 and index 2 that have identical content.

        In case of duplicates in an index, there is possibly more than one file in each of the
        pairs' elements.
        """
        with opened_storage(self):
            with contextlib.closing(self.connection.cursor()) as cursor:
                for row in cursor.execute(
                    "SELECT "
                    "  file1.hash,"
                    "  group_concat(DISTINCT file1.path),"
                    "  group_concat(DISTINCT file2.path) "
                    "FROM file1 JOIN file2 "
                    "  ON file1.hash == file2.hash "
                    "GROUP BY file1.hash"
                ):
                    fmap = FilesMap._make(row)

                    # unpack CVS fields:
                    yield fmap._replace(
                        files1=fmap.files1.split(","), files2=fmap.files2.split(",")
                    )

    def report_raw(self):
        click.echo()
        click.secho("Missing files:", underline=True, bold=True, fg="bright_magenta")
        click.echo("\n".join(f.path for f in self.iter_missing()))
        click.echo()
        click.secho("New files:", underline=True, bold=True, fg="bright_cyan")
        click.echo("\n".join(f.path for f in self.iter_new()))
