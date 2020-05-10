import logging
import pathlib

import click

from findex.db import DbExistsError
from findex.index import Index, Comparison


@click.group()
def cli():
    import daiquiri

    daiquiri.setup(level=logging.WARNING)


@cli.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option(
    "--db", type=click.Path(), default="findex.db", help="Path to generated index file."
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Flag, whether to allow overwriting index file.",
)
def index(directory, db, overwrite):
    """Create an hash-based file index for a directory tree.

    DIRECTORY is the path to the root of the file tree being indexed.
    """

    index_path = pathlib.Path(db).absolute()
    directory_path = pathlib.Path(directory).absolute()
    if overwrite:
        index_path.unlink(missing_ok=True)

    try:
        Index(index_path).create(directory_path)
    except DbExistsError:
        click.secho(
            f"The index {db!r} already exists, please choose another file or use the --overwrite "
            f"option.",
            fg="bright_red",
        )
    except Exception as ex:
        click.secho(f"An unexpected error occured: {ex}.", fg="bright_red")
        raise


@cli.command()
@click.argument("index1", type=click.Path(exists=True))
@click.argument("index2", type=click.Path(exists=True))
@click.option(
    "--db",
    type=click.Path(),
    default="fcomp.db",
    help="Path to generated comparison file.",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="Flag, whether to allow overwriting comparison file.",
)
def compare(index1, index2, db, overwrite):
    """Compare two file index files INDEX1 and INDEX2."""
    index1 = Index(pathlib.Path(index1))
    index2 = Index(pathlib.Path(index2))

    comparison_path = pathlib.Path(db).absolute()
    if overwrite:
        comparison_path.unlink(missing_ok=True)

    try:
        Comparison(comparison_path).create(index1, index2)
    except DbExistsError:
        click.secho(
            f"The comparison {db!r} already exists, please choose another file or use the "
            f"--overwrite option.",
            fg="bright_red",
        )
    except Exception as ex:
        click.secho(f"An unexpected error occured: {ex}.", fg="bright_red")
        raise


@cli.command()
@click.argument("comparison", type=click.Path(exists=True), default="fcomp.db")
def report(comparison):
    """Reporting.

    COMPARISON is the path to a comparison which is analyzed.
    """
    comparison_path = pathlib.Path(comparison).absolute()
    c = Comparison(comparison_path)
    c.report_raw()


if __name__ == "__main__":
    cli()
