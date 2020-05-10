"""File system utilities."""
import collections
import datetime
import hashlib
import logging
import mmap
import os
import pathlib
import typing as t

# fake hash values to identify non-hashable files:
FILEHASH_EMPTY = "_empty"
FILEHASH_INACCESSIBLE_FILE = "_inaccessible_file"
FILEHASH_WALK_ERROR = "_error: {message}"

FileDesc = collections.namedtuple("FileDesc", "path size fhash created modified")
"""Descriptor for a file in index."""

_logger = logging.getLogger(__name__)


def count_files(top: pathlib.Path, onerror=None) -> int:
    _logger.debug(f"Counting files in {top}.")

    count = 0
    for dirpath, dirnames, filenames in os.walk(top, onerror=onerror):
        count += len(filenames)

    return count


def count_bytes(top: pathlib.Path) -> int:
    count = 0
    for dirpath, _, filenames in os.walk(top):
        for filename in filenames:
            count += pathlib.Path(dirpath, filename).stat().st_size
    return count


def walk(top: pathlib.Path) -> t.Iterable[FileDesc]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    _logger.debug(f"Traversing directory {top} recursively.")

    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath)

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
                path=str(filepath.relative_to(top)),
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
