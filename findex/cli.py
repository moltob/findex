import logging
import pathlib

import click

from findex.index import Index, IndexExistsError


@click.group()
def cli():
    import daiquiri

    daiquiri.setup(level=logging.WARNING)


@cli.command()
@click.argument("directory", type=click.Path())
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

    index_path = pathlib.Path(db).resolve()
    directory_path = pathlib.Path(directory).resolve()
    if overwrite:
        index_path.unlink(missing_ok=True)

    try:
        findex = Index(index_path)
        findex.create()
        findex.add_directory(directory_path)
    except IndexExistsError:
        click.secho(
            f"The index {db!r} already exists, please choose another file or use the --overwrite "
            f"option.",
            fg="bright_red",
        )
    except Exception as ex:
        click.secho(
            f"An unexpected error occured: {ex}.", fg="bright_red",
        )
        raise


@cli.command()
def compare():
    """Compare files in two directory indexes."""


if __name__ == "__main__":
    cli()
