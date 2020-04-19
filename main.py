import logging
import pathlib

from findex import index_directory, count_files, count_bytes


def main():
    import colorama
    colorama.init()

    import daiquiri
    daiquiri.setup(level=logging.DEBUG)

    # findex = Index(pathlib.Path('test.db'))
    # findex.create()

    TEST_DIR = pathlib.Path(r'Q:\Britta Pagel Fotografie')

    def timed_main():
        list(index_directory(TEST_DIR))

    import timeit

    files_time = timeit.timeit(lambda: count_files(TEST_DIR), number=1)
    bytes_time = timeit.timeit(lambda: count_bytes(TEST_DIR), number=1)
    exec_time = timeit.timeit(timed_main, number=1)

    print(f'Count files:    {files_time:.3f} s')
    print(f'Count bytes:    {bytes_time:.3f} s')
    print(f'Execution time: {exec_time:.3f} s')


if __name__ == '__main__':
    main()
