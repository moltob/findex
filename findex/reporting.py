"""Reporting of a comparison result."""
import contextlib
import logging
import pathlib

import click
import xlsxwriter

from findex.fs import FileDesc
from findex.index import Comparison

_logger = logging.getLogger(__name__)

COL_WIDTH_PATH = 130
COL_WIDTH_SIZE = 15
COL_WIDTH_DATE = 25
COL_WIDTH_COUNT = 10
COL_WIDTH_HASH = 50


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
                "textlist": workbook.add_format({"text_wrap": True}),
            }

            self._write_files_worksheet("Missing Files", self.comparison.iter_missing())
            self._write_files_worksheet("Updated Files", self.comparison.iter_updated())
            self._write_files_worksheet("New Files", self.comparison.iter_new())
            self._write_moved_files_worksheet(
                "Moved Files", self.comparison.iter_content_groups()
            )

        self.workbook = self.formats = None

    def _write_files_worksheet(self, worksheet_name, files):
        click.secho(
            f"\nCreating worksheet {worksheet_name!r}.", bold=True, fg="bright_cyan"
        )

        worksheet = self.workbook.add_worksheet(worksheet_name)
        data = [(f.path, f.size, f.created, f.modified, f.fhash) for f in files]

        click.echo(f"{worksheet_name!r} has {len(data)} entries.")

        if not data:
            _logger.warning(
                f"Skipping worksheet {worksheet_name!r} as data set is empty."
            )
            worksheet.write_string(0, 0, f"No {worksheet_name!r} found.")
            return

        worksheet.set_column(0, 0, width=COL_WIDTH_PATH)
        worksheet.set_column(1, 1, width=COL_WIDTH_SIZE)
        worksheet.set_column(2, 3, width=COL_WIDTH_DATE)
        worksheet.set_column(4, 4, width=COL_WIDTH_HASH)

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

    def _write_moved_files_worksheet(self, worksheet_name, content_groups):
        click.secho(
            f"\nCreating worksheet {worksheet_name!r}.", bold=True, fg="bright_cyan"
        )
        data = [
            (
                len(g.files1),
                "\n".join(g.files1),
                len(g.files2),
                "\n".join(g.files2),
                g.size,
                g.fhash,
            )
            for g in content_groups
        ]
        click.echo(f"{worksheet_name!r} has {len(data)} entries.")

        if not data:
            _logger.warning(
                f"Skipping worksheet {worksheet_name!r} as data set is empty."
            )
            return

        worksheet = self.workbook.add_worksheet(worksheet_name)

        worksheet.set_column(0, 0, width=COL_WIDTH_COUNT)
        worksheet.set_column(1, 1, width=COL_WIDTH_PATH)
        worksheet.set_column(2, 2, width=COL_WIDTH_COUNT)
        worksheet.set_column(3, 3, width=COL_WIDTH_PATH)
        worksheet.set_column(4, 4, width=COL_WIDTH_SIZE)
        worksheet.set_column(5, 5, width=COL_WIDTH_HASH)

        worksheet.add_table(
            0,
            0,
            len(data),
            5,
            {
                "style": "Table Style Light 18",
                "data": data,
                "columns": [
                    {"header": "Duplicates 1"},
                    {
                        "header": "Original Location (Index 1)",
                        "format": self.formats["textlist"],
                    },
                    {"header": "Duplicates 2"},
                    {
                        "header": "New Location (Index 2)",
                        "format": self.formats["textlist"],
                    },
                    {"header": "Size (Bytes)"},
                    {"header": "Checksum (SHA1)", "format": self.formats["hash"]},
                ],
            },
        )
