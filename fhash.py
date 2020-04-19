"""Functions to compute file hashes based on content."""
import hashlib
import mmap
import os
import pathlib
import typing as t

from findex import FILEHASH_EMPTY, FileDesc


def count_files(top: pathlib.Path) -> int:
    count = 0
    for dirpath, dirnames, filenames in os.walk(top):
        count += len(filenames)
    return count


def index_directory(top: pathlib.Path) -> t.Iterable[t.Tuple[str, pathlib.Path, int]]:
    """Recurse given directory and for each non-empty file return content hash and path."""
    for dirpath, dirnames, filenames in os.walk(top):
        root = pathlib.Path(dirpath).absolute()
        for filename in filenames:
            filepath = root / filename
            filesize = filepath.stat().st_size

            if filesize == 0:
                filehash = FILEHASH_EMPTY
            else:
                filehash = compute_filehash(filepath)
            yield FileDesc(path=filepath, fhash=filehash, size=filesize)


def compute_filehash(filepath: pathlib.Path) -> str:
    with open(filepath, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
            sha1 = hashlib.sha1(data)
    return sha1.hexdigest()
