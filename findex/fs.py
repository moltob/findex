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
            stat = safe_file_access(filepath, lambda p: p.stat())
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
    with safe_file_access(filepath, lambda p: open(p, "rb")) as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()


def safe_file_access(path: pathlib.Path, path_func):
    """Trys to apply callable to path. Retries in case of long filenames."""
    try:
        return path_func(path)
    except OSError as ex:
        _logger.warning(f'Access to {path} failed (ex), retrying with relative path.')

        cwd = os.getcwd()
        try:
            # traverse down to file one folder at a time:
            for parent in reversed(path.parents):
                os.chdir(parent)

            return path_func(path.name)
        finally:
            os.chdir(cwd)