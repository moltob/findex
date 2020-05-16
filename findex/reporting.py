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

        self.workbook = None
        self.formats = None
        self.date_format = None
        self.currency_format = None

    def write(self, path: pathlib.Path):
        with contextlib.closing(xlsxwriter.Workbook(path)) as workbook:
            self.workbook = workbook

            # built-in formats:
            self.formats = {
                "currency": workbook.add_format({"num_format": 0x08}),
                "date": workbook.add_format({"num_format": 0x0E}),
                "datetime": workbook.add_format({"num_format": "dd/mm/yyyy hh:mm:ss"}),
                "hash": workbook.add_format(
                    {"font_name": "Courier New", "font_size": 10, "align": "center"}
                ),
            }

            self._write_new()

        self.workbook = self.formats = None

    def _write_new(self):
        worksheet = self.workbook.add_worksheet("New Files")

        worksheet.set_column(0, 0, width=80)
        worksheet.set_column(1, 1, width=15)
        worksheet.set_column(2, 3, width=25)
        worksheet.set_column(4, 4, width=50)

        data = []
        for file in self.comparison.iter_updated():
            data.append((file.path, file.size, file.created, file.modified, file.fhash))

        worksheet.add_table(
            0,
            0,
            len(data),
            len(FileDesc._fields) - 1,
            {
                "style": "Table Style Light 18",
                "data": data,
                "columns": [
                    {"header": "Path"},
                    {"header": "Size (Bytes)"},
                    {"header": "Created", "format": self.formats["datetime"]},
                    {"header": "Modified", "format": self.formats["datetime"]},
                    {"header": "Checksum (SHA1)", "format": self.formats["hash"]},
                ],
            },
        )
