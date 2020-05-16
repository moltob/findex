"""Reporting of a comparison result."""
import contextlib
import logging
import pathlib

import xlsxwriter

from findex.fs import FileDesc
from findex.index import Comparison

_logger = logging.getLogger(__name__)


class ComparisonReport:
    def __init__(self, comparison: Comparison):
        self.comparison = comparison

    def write(self, path: pathlib.Path):
        with contextlib.closing(xlsxwriter.Workbook(path)) as workbook:
            worksheet = workbook.add_worksheet("New Files")
            self._write_new(worksheet)

    def _write_new(self, worksheet):
        data = []
        for file in self.comparison.iter_updated():
            data.append((file.path, file.size, file.created, file.modified, file.fhash))

        worksheet.add_table(0, 0, len(data) + 1, 5, {"data": data})
