"""Reporting of a comparison result."""
import contextlib
import logging
import pathlib
import typing as t

import click
import xlsxwriter

from findex.db import META_DATE, META_VERSION
from findex.fs import FileDesc
from findex.index import Comparison, META_ROOT_RESOLVED

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

    def write(self, path: pathlib.Path):
        with contextlib.closing(xlsxwriter.Workbook(path)) as workbook:
            self.workbook = workbook

            # built-in formats:
            self.formats = {
                "datetime": workbook.add_format(
                    {"num_format": "dd/mm/yyyy hh:mm:ss", "valign": "top"}
                ),
                "hash": workbook.add_format(
                    {
                        "font_name": "Courier New",
                        "font_size": 10,
                        "align": "center",
                        "valign": "top",
                    }
                ),
                "textlist": workbook.add_format({"text_wrap": True, "valign": "top"}),
                "number": workbook.add_format({"num_format": 0x01, "valign": "top"}),
                "header": workbook.add_format({"font_size": 20, "bold": True}),
                "summary_key": workbook.add_format({"align": "right", "bold": True}),
                "summary_value": workbook.add_format({"align": "left"}),
            }

            summary_worksheet = self.workbook.add_worksheet("Summary")

            missing_files = self._write_files_worksheet(
                "Missing Files", self.comparison.iter_missing()
            )
            updated_files = self._write_files_worksheet(
                "Updated Files", self.comparison.iter_updated()
            )
            new_files = self._write_files_worksheet(
                "New Files", self.comparison.iter_new()
            )
            moved_groups = self._write_moved_files_worksheet("Moved Files")

            self._write_summary_worksheet(
                summary_worksheet,
                missing_files=missing_files,
                updated_files=updated_files,
                new_files=new_files,
                moved_groups=moved_groups,
            )

        self.workbook = self.formats = None

    def _write_files_worksheet(self, worksheet_name: str, files: t.Iterable[FileDesc]):
        click.secho(
            f"\nCreating worksheet {worksheet_name!r}.", bold=True, fg="bright_cyan"
        )

        worksheet = self.workbook.add_worksheet(worksheet_name)
        data = [(f.path, f.size, f.created, f.modified, f.fhash) for f in files]

        click.echo(f"{worksheet_name!r} has {len(data)} entries.")

        if not data:
            worksheet.write_string(0, 0, f"No {worksheet_name!r} found.")
            return 0

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
                    {"header": "Path", "format": self.formats["textlist"]},
                    {"header": "Size (Bytes)", "format": self.formats["number"]},
                    {"header": "Created", "format": self.formats["datetime"]},
                    {"header": "Modified", "format": self.formats["datetime"]},
                    {"header": "Checksum (SHA1)", "format": self.formats["hash"]},
                ],
            },
        )

        return len(data)

    def _write_moved_files_worksheet(self, worksheet_name: str):
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
            for g in self.comparison.iter_content_groups()
        ]
        click.echo(f"{worksheet_name!r} has {len(data)} entries.")

        if not data:
            _logger.warning(
                f"Skipping worksheet {worksheet_name!r} as data set is empty."
            )
            return 0

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
                    {"header": "Duplicates 1", "format": self.formats["number"]},
                    {
                        "header": "Original Location (Index 1)",
                        "format": self.formats["textlist"],
                    },
                    {"header": "Duplicates 2", "format": self.formats["number"]},
                    {
                        "header": "New Location (Index 2)",
                        "format": self.formats["textlist"],
                    },
                    {"header": "Size (Bytes)", "format": self.formats["number"]},
                    {"header": "Checksum (SHA1)", "format": self.formats["hash"]},
                ],
            },
        )

        return len(data)

    def _write_summary_worksheet(
        self,
        worksheet,
        *,
        missing_files: int,
        updated_files: int,
        new_files: int,
        moved_groups: int,
    ):
        click.secho(
            f"\nCreating worksheet {worksheet.name!r}.", bold=True, fg="bright_cyan"
        )

        worksheet.write_string(
            0, 0, "Directory Tree Comparison", cell_format=self.formats["header"]
        )

        worksheet.set_column(0, 0, width=60)
        worksheet.set_column(1, 1, width=COL_WIDTH_PATH)

        with contextlib.closing(self.comparison.open()):
            data = [
                ["Created:", self.comparison.get_meta(META_DATE)],
                ["Version:", self.comparison.get_meta(META_VERSION)],
                ["", ""],
                ["Index 1:", self.comparison.get_index_meta(META_ROOT_RESOLVED, "1")],
                ["Created:", self.comparison.get_index_meta(META_DATE, "1")],
                ["", ""],
                ["Index 2:", self.comparison.get_index_meta(META_ROOT_RESOLVED, "2")],
                ["Created:", self.comparison.get_index_meta(META_DATE, "2")],
                ["", ""],
                ["Missing files:", missing_files],
                ["Updated files:", updated_files],
                ["New files:", new_files],
                ["Moved files with identical content:", moved_groups],
            ]

        offset = 2
        for index, entry in enumerate(data):
            key, value = entry
            worksheet.write_string(
                offset + index, 0, key, cell_format=self.formats["summary_key"]
            )
            worksheet.write(offset + index, 1, value, self.formats["summary_value"])
