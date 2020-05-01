"""Comparison of two indexes."""
import pathlib

from findex.util import Storage


class Comparator(Storage):
    """Comparison of two index databases."""

    def create(self):
        """Create and open sqlite DB with index schema."""
        self.create_db()

    def compare(self, index1: pathlib.Path, index2: pathlib.Path):
        pass
