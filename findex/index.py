"""Index of files in a directory structure."""
import collections
import contextlib
import datetime
import hashlib
import logging
import mmap
import os
import pathlib
import sqlite3
import typing as t

import click
import pkg_resources
import tqdm

from findex.util import Storage

# fake hash values to identify non-hashable files:
FILEHASH_EMPTY = "_empty"
FILEHASH_INACCESSIBLE_FILE = "_inaccessible_file"
FILEHASH_WALK_ERROR = "_error: {message}"

FileDesc = collections.namedtuple("FileDesc", "path size fhash created modified")
"""Descriptor for a file in index."""

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
        except sqlite3.OperationalError as ex:
            _logger.error(f"Cannot add file to database: {filedesc}")
            raise


def count_files(top: pathlib.Path, onerror=None) -> int:
    _logger.debug(f"Counting files in {top}.")

    count = 0
    for dirpath, dirnames, filenames in os.walk(top, onerror=onerror):
        count += len(filenames)

    return count


def count_bytes(top: pathlib.Path) -> int:
    count = 0
    for dirpath, dirnames, filenames in os.walk(top):
        for filename in filenames:
            count += pathlib.Path(dirpath, filename).stat().st_size
    return count


def walk(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path, int]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    _logger.debug(f"Traversing directory {top} recursively.")

    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath)

        for dirname in dirnames:
            # check accessibility of folder to make user aware:
            dirpath = root / dirname

        for filename in filenames:
            filepath = root / filename
            stat = filepath.stat()
            filesize = stat.st_size

            if filesize == 0:
                filehash = FILEHASH_EMPTY
            else:
                try:
                    filehash = compute_filehash(filepath)
                except PermissionError:
                    _logger.warning(f"File inaccessible: {filepath}.")
                    filehash = FILEHASH_INACCESSIBLE_FILE

            _logger.debug(f"{filehash} {filepath}")
            yield FileDesc(
                path=str(filepath),
                fhash=filehash,
                size=filesize,
                created=datetime.datetime.fromtimestamp(stat.st_ctime),
                modified=datetime.datetime.fromtimestamp(stat.st_mtime),
            )


def compute_filehash(filepath: pathlib.Path) -> str:
    with open(filepath, "rb") as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()
